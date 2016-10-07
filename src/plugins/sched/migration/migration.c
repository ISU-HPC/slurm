/*****************************************************************************\
 *  migration.c - simple migration scheduler plugin.
 *
 *  If a partition does not have root only access and nodes are not shared
 *  then raise the priority of pending jobs if doing so does not adversely
 *  effect the expected initiation of any higher priority job. We do not alter
 *  a job's required or excluded node list, so this is a conservative
 *  algorithm.
 *
 *  For example, consider a cluster "lx[01-08]" with one job executing on
 *  nodes "lx[01-04]". The highest priority pending job requires five nodes
 *  including "lx05". The next highest priority pending job requires any
 *  three nodes. Without explicitly forcing the second job to use nodes
 *  "lx[06-08]", we can't start it without possibly delaying the higher
 *  priority job.
 *****************************************************************************
 *  Copyright (C) 2003-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#include "config.h"

#if HAVE_SYS_PRCTL_H
#  include <sys/prctl.h>
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "slurm/slurm.h"
#include "slurm/slurm_errno.h"

#include "src/common/assoc_mgr.h"
#include "src/common/list.h"
#include "src/common/macros.h"
#include "src/common/node_select.h"
#include "src/common/parse_time.h"
#include "src/common/power.h"
#include "src/common/read_config.h"
#include "src/common/slurm_accounting_storage.h"
#include "src/common/slurm_mcs.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"

#include "src/slurmctld/acct_policy.h"
#include "src/slurmctld/burst_buffer.h"
#include "src/slurmctld/front_end.h"
#include "src/slurmctld/job_scheduler.h"
#include "src/slurmctld/licenses.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/node_scheduler.h"
#include "src/slurmctld/preempt.h"
#include "src/slurmctld/reservation.h"
#include "src/slurmctld/slurmctld.h"
#include "src/slurmctld/srun_comm.h"
#include "src/plugins/slurmctld/job_migration/job_migration.h"

#include "migration.h"


#define MIGRATION_INTERVAL	30
#define MIGRATION_RESOLUTION	60
#define MIGRATION_WINDOW		(24 * 60 * 60)
#define BF_MAX_USERS		1000
#define BF_MAX_JOB_ARRAY_RESV	20

#define SLURMCTLD_THREAD_LIMIT	5
#define SCHED_TIMEOUT		2000000	/* time in micro-seconds */
#define YIELD_SLEEP		500000;	/* time in micro-seconds */

typedef struct node_space_map {
	time_t begin_time;
	time_t end_time;
	bitstr_t *avail_bitmap;
	int next;	/* next record, by time, zero termination */
} node_space_map_t;

/* Diag statistics */
extern diag_stats_t slurmctld_diag_stats;
uint32_t bf_sleep_usec = 0;

/*********************** local variables *********************/
static bool stop_migration = false;
static pthread_mutex_t thread_flag_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t term_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  term_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t config_lock = PTHREAD_MUTEX_INITIALIZER;
static bool config_flag = false;
static uint64_t debug_flags = 0;
static int migration_interval = MIGRATION_INTERVAL;
static int migration_resolution = MIGRATION_RESOLUTION;
static int migration_window = MIGRATION_WINDOW;
static int bf_max_job_array_resv = BF_MAX_JOB_ARRAY_RESV;
static int bf_min_age_reserve = 0;
static uint32_t bf_min_prio_reserve = 0;
static int max_migration_job_cnt = 100;
static int max_migration_job_per_part = 0;
static int max_migration_job_per_user = 0;
static int max_migration_jobs_start = 0;
static bool migration_continue = false;
static int defer_rpc_cnt = 0;
static int sched_timeout = SCHED_TIMEOUT;
static int yield_sleep   = YIELD_SLEEP;

/*********************** local functions *********************/
static void _add_reservation(uint32_t start_time, uint32_t end_reserve,
			     bitstr_t *res_bitmap,
			     node_space_map_t *node_space,
			     int *node_space_recs);
static int  _attempt_migration(void);
extern List _build_running_job_queue();
static bool _can_be_allocated(struct job_record *job_ptr);
static bool _can_be_migrated(struct job_record *job_ptr);
static int  _delta_tv(struct timeval *tv);
static void _do_diag_stats(struct timeval *tv1, struct timeval *tv2);
static void _load_config(void);
static bool _many_pending_rpcs(void);
static bool _migration_will_save_time(struct job_record *job_ptr);
static bool _more_work(time_t last_migration_time);
static uint32_t _my_sleep(int usec);
static int  _num_feature_count(struct job_record *job_ptr, bool *has_xor);
static int  _try_sched(struct job_record *job_ptr, bitstr_t **avail_bitmap,
		       uint32_t min_nodes, uint32_t max_nodes,
		       uint32_t req_nodes, bitstr_t *exc_core_bitmap);
void _printBitStr(void *data, int size);

/* funciones que he quitado */
/*
static void _clear_job_start_times(void);

static bool _job_part_valid(struct job_record *job_ptr,
			    struct part_record *part_ptr);

static void _reset_job_time_limit(struct job_record *job_ptr, time_t now,
					node_space_map_t *node_space);

static int  _start_job(struct job_record *job_ptr, bitstr_t *avail_bitmap);

static bool _test_resv_overlap(node_space_map_t *node_space,
			       bitstr_t *use_bitmap, uint32_t start_time,
			       uint32_t end_reserve);

static int  _yield_locks(int usec);
*/


/* Log resources to be allocated to a pending job */

/* MANUEL: ESTO NO SE USABA, FUERA
static void _dump_job_sched(struct job_record *job_ptr, time_t end_time,
			    bitstr_t *avail_bitmap)
{
	char begin_buf[32], end_buf[32], *node_list;

	slurm_make_time_str(&job_ptr->start_time, begin_buf, sizeof(begin_buf));
	slurm_make_time_str(&end_time, end_buf, sizeof(end_buf));
	node_list = bitmap2node_name(avail_bitmap);
	info("Job %u to start at %s, end at %s on %s",
	     job_ptr->job_id, begin_buf, end_buf, node_list);
	xfree(node_list);
}


*/


/* MANUEL: ESTO NO SE USABA, FUERA
static void _dump_job_test(struct job_record *job_ptr, bitstr_t *avail_bitmap,
			   time_t start_time)
{
	char begin_buf[32], *node_list;

	if (start_time == 0)
		strcpy(begin_buf, "NOW");
	else
		slurm_make_time_str(&start_time, begin_buf, sizeof(begin_buf));
	node_list = bitmap2node_name(avail_bitmap);
	info("Test job %u at %s on %s", job_ptr->job_id, begin_buf, node_list);
	xfree(node_list);
}
*/

/* Log resource allocate table */

/*  MANUEL: ESTO NO SE USABA, FUERA
static void _dump_node_space_table(node_space_map_t *node_space_ptr)
{
	int i = 0;
	char begin_buf[32], end_buf[32], *node_list;

	info("=========================================");
	while (1) {
		slurm_make_time_str(&node_space_ptr[i].begin_time,
				    begin_buf, sizeof(begin_buf));
		slurm_make_time_str(&node_space_ptr[i].end_time,
				    end_buf, sizeof(end_buf));
		node_list = bitmap2node_name(node_space_ptr[i].avail_bitmap);
		info("Begin:%s End:%s Nodes:%s",
		     begin_buf, end_buf, node_list);
		xfree(node_list);
		if ((i = node_space_ptr[i].next) == 0)
			break;
	}
	info("=========================================");
}
*/

/* MANUEL: ESTO NO SE USABA, FUERA
static void _set_job_time_limit(struct job_record *job_ptr, uint32_t new_limit)
{
	job_ptr->time_limit = new_limit;
	if (job_ptr->time_limit == NO_VAL)
		job_ptr->limit_set.time = 0;

}
*/
/*
 * _many_pending_rpcs - Determine if slurmctld is busy with many active RPCs
 * RET - True if slurmctld currently has more than SLURMCTLD_THREAD_LIMIT
 *	 active RPCs
 */
static bool _many_pending_rpcs(void)
{
	//info("thread_count = %u", slurmctld_config.server_thread_count);
	if ((defer_rpc_cnt > 0) &&
	    (slurmctld_config.server_thread_count >= defer_rpc_cnt))
		return true;
	return false;
}

/* test if job has feature count specification */
static int _num_feature_count(struct job_record *job_ptr, bool *has_xor)
{
	struct job_details *detail_ptr = job_ptr->details;
	int rc = 0;
	ListIterator feat_iter;
	job_feature_t *feat_ptr;

	if (detail_ptr->feature_list == NULL)	/* no constraints */
		return rc;

	feat_iter = list_iterator_create(detail_ptr->feature_list);
	while ((feat_ptr = (job_feature_t *) list_next(feat_iter))) {
		if (feat_ptr->count)
			rc++;
		if (feat_ptr->op_code == FEATURE_OP_XOR)
			*has_xor = true;
	}
	list_iterator_destroy(feat_iter);

	return rc;
}

/* Attempt to schedule a specific job on specific available nodes
 * IN job_ptr - job to schedule
 * IN/OUT avail_bitmap - nodes available/selected to use
 * IN exc_core_bitmap - cores which can not be used
 * RET SLURM_SUCCESS on success, otherwise an error code
 */


//MANUEL esto puede ser importante para la migracion
static int  _try_sched(struct job_record *job_ptr, bitstr_t **avail_bitmap,
		       uint32_t min_nodes, uint32_t max_nodes,
		       uint32_t req_nodes, bitstr_t *exc_core_bitmap)
{
	bitstr_t *low_bitmap = NULL, *tmp_bitmap = NULL;
	int rc = SLURM_SUCCESS;
	bool has_xor = false;
	int feat_cnt = _num_feature_count(job_ptr, &has_xor);
	struct job_details *detail_ptr = job_ptr->details;
	List preemptee_candidates = NULL;
	List preemptee_job_list = NULL;
	ListIterator feat_iter;
	job_feature_t *feat_ptr;

	if (feat_cnt) {
		/* Ideally schedule the job feature by feature,
		 * but I don't want to add that complexity here
		 * right now, so clear the feature counts and try
		 * to schedule. This will work if there is only
		 * one feature count. It should work fairly well
		 * in cases where there are multiple feature
		 * counts. */
		int i = 0, list_size;
		uint16_t *feat_cnt_orig = NULL, high_cnt = 0;

		/* Clear the feature counts */
		list_size = list_count(detail_ptr->feature_list);
		feat_cnt_orig = xmalloc(sizeof(uint16_t) * list_size);
		feat_iter = list_iterator_create(detail_ptr->feature_list);
		while ((feat_ptr = (job_feature_t *) list_next(feat_iter))) {
			high_cnt = MAX(high_cnt, feat_ptr->count);
			feat_cnt_orig[i++] = feat_ptr->count;
			feat_ptr->count = 0;
		}
		list_iterator_destroy(feat_iter);

		if ((job_req_node_filter(job_ptr, *avail_bitmap, true) !=
		     SLURM_SUCCESS) ||
		    (bit_set_count(*avail_bitmap) < high_cnt)) {
			rc = ESLURM_NODES_BUSY;
		} else {
			preemptee_candidates =
				slurm_find_preemptable_jobs(job_ptr);
			rc = select_g_job_test(job_ptr, *avail_bitmap,
					       high_cnt, max_nodes, req_nodes,
					       SELECT_MODE_WILL_RUN,
					       preemptee_candidates,
					       &preemptee_job_list,
					       exc_core_bitmap);
			FREE_NULL_LIST(preemptee_job_list);
		}

		/* Restore the feature counts */
		i = 0;
		feat_iter = list_iterator_create(detail_ptr->feature_list);
		while ((feat_ptr = (job_feature_t *) list_next(feat_iter))) {
			feat_ptr->count = feat_cnt_orig[i++];
		}
		list_iterator_destroy(feat_iter);
		xfree(feat_cnt_orig);
	} else if (has_xor) {
		/* Cache the feature information and test the individual
		 * features, one at a time */
		job_feature_t feature_base;
		List feature_cache = detail_ptr->feature_list;
		time_t low_start = 0;

		detail_ptr->feature_list = list_create(NULL);
		feature_base.count = 0;
		feature_base.op_code = FEATURE_OP_END;
		list_append(detail_ptr->feature_list, &feature_base);

		tmp_bitmap = bit_copy(*avail_bitmap);
		feat_iter = list_iterator_create(feature_cache);
		while ((feat_ptr = (job_feature_t *) list_next(feat_iter))) {
			feature_base.name = feat_ptr->name;
			if ((job_req_node_filter(job_ptr, *avail_bitmap, true)
			     == SLURM_SUCCESS) &&
			    (bit_set_count(*avail_bitmap) >= min_nodes)) {
				preemptee_candidates =
					slurm_find_preemptable_jobs(job_ptr);
				rc = select_g_job_test(job_ptr, *avail_bitmap,
						       min_nodes, max_nodes,
						       req_nodes,
						       SELECT_MODE_WILL_RUN,
						       preemptee_candidates,
						       &preemptee_job_list,
						       exc_core_bitmap);
				FREE_NULL_LIST(preemptee_job_list);
				if ((rc == SLURM_SUCCESS) &&
				    ((low_start == 0) ||
				     (low_start > job_ptr->start_time))) {
					low_start = job_ptr->start_time;
					low_bitmap = *avail_bitmap;
					*avail_bitmap = NULL;
				}
			}
			FREE_NULL_BITMAP(*avail_bitmap);
			*avail_bitmap = bit_copy(tmp_bitmap);
		}
		list_iterator_destroy(feat_iter);
		FREE_NULL_BITMAP(tmp_bitmap);
		if (low_start) {
			job_ptr->start_time = low_start;
			rc = SLURM_SUCCESS;
			*avail_bitmap = low_bitmap;
		} else {
			rc = ESLURM_NODES_BUSY;
			FREE_NULL_BITMAP(low_bitmap);
		}

		/* Restore the original feature information */
		list_destroy(detail_ptr->feature_list);
		detail_ptr->feature_list = feature_cache;
	} else if (detail_ptr->feature_list) {
		if ((job_req_node_filter(job_ptr, *avail_bitmap, true) !=
		     SLURM_SUCCESS) ||
		    (bit_set_count(*avail_bitmap) < min_nodes)) {
			rc = ESLURM_NODES_BUSY;
		} else {
			preemptee_candidates =
					slurm_find_preemptable_jobs(job_ptr);
			rc = select_g_job_test(job_ptr, *avail_bitmap,
					       min_nodes, max_nodes, req_nodes,
					       SELECT_MODE_WILL_RUN,
					       preemptee_candidates,
					       &preemptee_job_list,
					       exc_core_bitmap);
			FREE_NULL_LIST(preemptee_job_list);
		}
	} else {
		/* Try to schedule the job. First on dedicated nodes
		 * then on shared nodes (if so configured). */
		uint16_t orig_shared;
		time_t now = time(NULL);
		char str[100];

		preemptee_candidates = slurm_find_preemptable_jobs(job_ptr);
		orig_shared = job_ptr->details->share_res;
		job_ptr->details->share_res = 0;
		tmp_bitmap = bit_copy(*avail_bitmap);

		if (exc_core_bitmap) {
			bit_fmt(str, (sizeof(str) - 1), exc_core_bitmap);
			debug2("%s exclude core bitmap: %s", __func__, str);
		}

		rc = select_g_job_test(job_ptr, *avail_bitmap, min_nodes,
				       max_nodes, req_nodes,
				       SELECT_MODE_WILL_RUN,
				       preemptee_candidates,
				       &preemptee_job_list,
				       exc_core_bitmap);
		FREE_NULL_LIST(preemptee_job_list);

		job_ptr->details->share_res = orig_shared;

		if (((rc != SLURM_SUCCESS) || (job_ptr->start_time > now)) &&
		    (orig_shared != 0)) {
			FREE_NULL_BITMAP(*avail_bitmap);
			*avail_bitmap = tmp_bitmap;
			rc = select_g_job_test(job_ptr, *avail_bitmap,
					       min_nodes, max_nodes, req_nodes,
					       SELECT_MODE_WILL_RUN,
					       preemptee_candidates,
					       &preemptee_job_list,
					       exc_core_bitmap);
			FREE_NULL_LIST(preemptee_job_list);
		} else
			FREE_NULL_BITMAP(tmp_bitmap);
	}

	FREE_NULL_LIST(preemptee_candidates);
	return rc;
}

/* Terminate migration_agent */
extern void stop_migration_agent(void)
{
	slurm_mutex_lock(&term_lock);
	stop_migration = true;
	slurm_cond_signal(&term_cond);
	slurm_mutex_unlock(&term_lock);
}

/* Return the number of micro-seconds between now and argument "tv" */
static int _delta_tv(struct timeval *tv)
{
	struct timeval now = {0, 0};
	int delta_t;

	if (gettimeofday(&now, NULL))
		return 1;		/* Some error */

	delta_t  = (now.tv_sec - tv->tv_sec) * 1000000;
	delta_t += (now.tv_usec - tv->tv_usec);
	return delta_t;
}

/* Sleep for at least specified time, returns actual sleep time in usec */
static uint32_t _my_sleep(int usec)
{
	int64_t nsec;
	uint32_t sleep_time = 0;
	struct timespec ts = {0, 0};
	struct timeval  tv1 = {0, 0}, tv2 = {0, 0};

	if (gettimeofday(&tv1, NULL)) {		/* Some error */
		sleep(1);
		return 1000000;
	}

	nsec  = tv1.tv_usec + usec;
	nsec *= 1000;
	ts.tv_sec  = tv1.tv_sec + (nsec / 1000000000);
	ts.tv_nsec = nsec % 1000000000;
	slurm_mutex_lock(&term_lock);
	if (!stop_migration)
		slurm_cond_timedwait(&term_cond, &term_lock, &ts);
	slurm_mutex_unlock(&term_lock);
	if (gettimeofday(&tv2, NULL))
		return usec;
	sleep_time = (tv2.tv_sec - tv1.tv_sec) * 1000000;
	sleep_time += tv2.tv_usec;
	sleep_time -= tv1.tv_usec;
	return sleep_time;
}

static void _load_config(void)
{
	char *sched_params, *tmp_ptr;

	sched_params = slurm_get_sched_params();
	debug_flags  = slurm_get_debug_flags();

	if (sched_params && (tmp_ptr = strstr(sched_params, "bf_interval="))) {
		migration_interval = atoi(tmp_ptr + 12);
		if (migration_interval < 1) {
			error("Invalid SchedulerParameters bf_interval: %d",
			      migration_interval);
			migration_interval = MIGRATION_INTERVAL;
		}
	} else {
		migration_interval = MIGRATION_INTERVAL;
	}

	if (sched_params && (tmp_ptr = strstr(sched_params, "bf_window="))) {
		migration_window = atoi(tmp_ptr + 10) * 60;  /* mins to secs */
		if (migration_window < 1) {
			error("Invalid SchedulerParameters bf_window: %d",
			      migration_window);
			migration_window = MIGRATION_WINDOW;
		}
	} else {
		migration_window = MIGRATION_WINDOW;
	}

	/* "max_job_bf" replaced by "bf_max_job_test" in version 14.03 and
	 * can be removed later. Only "bf_max_job_test" is documented. */
	if (sched_params && (tmp_ptr=strstr(sched_params, "bf_max_job_test=")))
		max_migration_job_cnt = atoi(tmp_ptr + 16);
	else if (sched_params && (tmp_ptr=strstr(sched_params, "max_job_bf=")))
		max_migration_job_cnt = atoi(tmp_ptr + 11);
	else
		max_migration_job_cnt = 100;
	if (max_migration_job_cnt < 1) {
		error("Invalid SchedulerParameters bf_max_job_test: %d",
		      max_migration_job_cnt);
		max_migration_job_cnt = 100;
	}

	if (sched_params && (tmp_ptr=strstr(sched_params, "bf_resolution="))) {
		migration_resolution = atoi(tmp_ptr + 14);
		if (migration_resolution < 1) {
			error("Invalid SchedulerParameters bf_resolution: %d",
			      migration_resolution);
			migration_resolution = MIGRATION_RESOLUTION;
		}
	} else {
		migration_resolution = MIGRATION_RESOLUTION;
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_max_job_array_resv="))) {
		bf_max_job_array_resv = atoi(tmp_ptr + 22);
		if (bf_max_job_array_resv < 0) {
			error("Invalid SchedulerParameters bf_max_job_array_resv: %d",
			      bf_max_job_array_resv);
			bf_max_job_array_resv = BF_MAX_JOB_ARRAY_RESV;
		}
	} else {
		bf_max_job_array_resv = BF_MAX_JOB_ARRAY_RESV;
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_max_job_part="))) {
		max_migration_job_per_part = atoi(tmp_ptr + 16);
		if (max_migration_job_per_part < 0) {
			error("Invalid SchedulerParameters bf_max_job_part: %d",
			      max_migration_job_per_part);
			max_migration_job_per_part = 0;
		}
	} else {
		max_migration_job_per_part = 0;
	}
	if ((max_migration_job_per_part != 0) &&
	    (max_migration_job_per_part >= max_migration_job_cnt)) {
		error("bf_max_job_part >= bf_max_job_test (%u >= %u)",
		      max_migration_job_per_part, max_migration_job_cnt);
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_max_job_start="))) {
		max_migration_jobs_start = atoi(tmp_ptr + 17);
		if (max_migration_jobs_start < 0) {
			error("Invalid SchedulerParameters bf_max_job_start: %d",
			      max_migration_jobs_start);
			max_migration_jobs_start = 0;
		}
	} else {
		max_migration_jobs_start = 0;
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_max_job_user="))) {
		max_migration_job_per_user = atoi(tmp_ptr + 16);
		if (max_migration_job_per_user < 0) {
			error("Invalid SchedulerParameters bf_max_job_user: %d",
			      max_migration_job_per_user);
			max_migration_job_per_user = 0;
		}
	} else {
		max_migration_job_per_user = 0;
	}
	if ((max_migration_job_per_user != 0) &&
	    (max_migration_job_per_user >= max_migration_job_cnt)) {
		error("bf_max_job_user >= bf_max_job_test (%u >= %u)",
		      max_migration_job_per_user, max_migration_job_cnt);
	}

	bf_min_age_reserve = 0;
	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_min_age_reserve="))) {
		int min_age = atoi(tmp_ptr + 19);
		if (min_age < 0) {
			error("Invalid SchedulerParameters bf_min_age_reserve: %d",
			      min_age);
		} else {
			bf_min_age_reserve = min_age;
		}
	}

	bf_min_prio_reserve = 0;
	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_min_prio_reserve="))) {
		int64_t min_prio = (int64_t) atoll(tmp_ptr + 20);
		if (min_prio < 0) {
			error("Invalid SchedulerParameters bf_min_prio_reserve: %"PRIi64,
			      min_prio);
		} else {
			bf_min_prio_reserve = (uint32_t) min_prio;
		}
	}

	/* bf_continue makes migration continue where it was if interrupted */
	if (sched_params && (strstr(sched_params, "bf_continue"))) {
		migration_continue = true;
	} else {
		migration_continue = false;
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_yield_interval="))) {
		sched_timeout = atoi(tmp_ptr + 18);
		if (sched_timeout <= 0) {
			error("Invalid migration scheduler bf_yield_interval: %d",
			      sched_timeout);
			sched_timeout = SCHED_TIMEOUT;
		}
	} else {
		sched_timeout = SCHED_TIMEOUT;
	}

	if (sched_params &&
	    (tmp_ptr = strstr(sched_params, "bf_yield_sleep="))) {
		yield_sleep = atoi(tmp_ptr + 15);
		if (yield_sleep <= 0) {
			error("Invalid migration scheduler bf_yield_sleep: %d",
			      yield_sleep);
			yield_sleep = YIELD_SLEEP;
		}
	} else {
		yield_sleep = YIELD_SLEEP;
	}

	if (sched_params && (tmp_ptr = strstr(sched_params, "max_rpc_cnt=")))
		defer_rpc_cnt = atoi(tmp_ptr + 12);
	else if (sched_params &&
		 (tmp_ptr = strstr(sched_params, "max_rpc_count=")))
		defer_rpc_cnt = atoi(tmp_ptr + 14);
	else
		defer_rpc_cnt = 0;
	if (defer_rpc_cnt < 0) {
		error("Invalid SchedulerParameters max_rpc_cnt: %d",
		      defer_rpc_cnt);
		defer_rpc_cnt = 0;
	}

	xfree(sched_params);
}

/* Note that slurm.conf has changed */
extern void migration_reconfig(void)
{
	slurm_mutex_lock(&config_lock);
	config_flag = true;
	slurm_mutex_unlock(&config_lock);
}

/* Update migration scheduling statistics
 * IN tv1 - start time
 * IN tv2 - end (current) time
 */
static void _do_diag_stats(struct timeval *tv1, struct timeval *tv2)
{
	uint32_t delta_t, real_time;

	delta_t  = (tv2->tv_sec - tv1->tv_sec) * 1000000;
	delta_t +=  tv2->tv_usec;
	delta_t -=  tv1->tv_usec;
	real_time = delta_t - bf_sleep_usec;

	slurmctld_diag_stats.bf_cycle_counter++;
	slurmctld_diag_stats.bf_cycle_sum += real_time;
	slurmctld_diag_stats.bf_cycle_last = real_time;

	slurmctld_diag_stats.bf_depth_sum += slurmctld_diag_stats.bf_last_depth;
	slurmctld_diag_stats.bf_depth_try_sum +=
		slurmctld_diag_stats.bf_last_depth_try;
	if (slurmctld_diag_stats.bf_cycle_last >
	    slurmctld_diag_stats.bf_cycle_max) {
		slurmctld_diag_stats.bf_cycle_max = slurmctld_diag_stats.
						    bf_cycle_last;
	}

	slurmctld_diag_stats.mg_active = 0;
}


/* migration_agent - detached thread periodically attempts to migration jobs */
extern void *migration_agent(void *args)
{
	time_t now;
	double wait_time;
	static time_t last_migration_time = 0;
	/* Read config and partitions; Write jobs and nodes */
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, WRITE_LOCK, READ_LOCK, NO_LOCK };
	bool load_config;
	bool short_sleep = false;

#if HAVE_SYS_PRCTL_H
	if (prctl(PR_SET_NAME, "bckfl", NULL, NULL, NULL) < 0) {
		error("%s: cannot set my name to %s %m", __func__, "migration");
	}
#endif
	_load_config();
	last_migration_time = time(NULL);
	while (!stop_migration) {
		if (short_sleep)
			_my_sleep(1000000);
		else
			_my_sleep(migration_interval * 1000000);
		if (stop_migration)
			break;
		slurm_mutex_lock(&config_lock);
		if (config_flag) {
			config_flag = false;
			load_config = true;
		} else {
			load_config = false;
		}
		slurm_mutex_unlock(&config_lock);
		if (load_config)
			_load_config();
		now = time(NULL);
		wait_time = difftime(now, last_migration_time);
		if ((wait_time < migration_interval) ||
		    job_is_completing() || _many_pending_rpcs() ||
		    !avail_front_end(NULL) || !_more_work(last_migration_time)) {
			short_sleep = true;
			continue;
		}
		lock_slurmctld(all_locks);

		//MANUEL: AQUI ES EL PARALELISMO. A VER QUE PASA
		//(void) _attempt_migration();


		pthread_t inc_x_thread;
		/* create a second thread which executes inc_x(&x) */
		if(pthread_create(&inc_x_thread, NULL, _attempt_migration, NULL)) {
			debug ("MANUEL I broke it LOL");
			return NULL;
		}
		else{
			debug ("MANUEL it seems we have created a migration thread, now what? what happens with concurrency? clean after thread???");
		}

		last_migration_time = time(NULL);
		(void) bb_g_job_try_stage_in();
		unlock_slurmctld(all_locks);
		short_sleep = false;
	}
	return NULL;
}

/* Clear the start_time for all pending jobs. This is used to ensure that a job which
 * can run in multiple partitions has its start_time set to the smallest
 * value in any of those partitions. */
/*MANUEL: ESTO NO SE USABA, FUERA
static void _clear_job_start_times(void)
{
	ListIterator job_iterator;
	struct job_record *job_ptr;

	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = (struct job_record *) list_next(job_iterator))) {
		if (IS_JOB_PENDING(job_ptr))
			job_ptr->start_time = 0;
	}
	list_iterator_destroy(job_iterator);
}

*/

/* Return non-zero to break the migration loop if change in job, node or
 * partition state or the migration scheduler needs to be stopped. */
static int _yield_locks(int usec)
{
	slurmctld_lock_t all_locks = {
		READ_LOCK, WRITE_LOCK, WRITE_LOCK, READ_LOCK, NO_LOCK };
	time_t job_update, node_update, part_update;
	bool load_config = false;
	int max_rpc_cnt;

	max_rpc_cnt = MAX((defer_rpc_cnt / 10), 20);
	job_update  = last_job_update;
	node_update = last_node_update;
	part_update = last_part_update;

	unlock_slurmctld(all_locks);
	while (!stop_migration) {
		bf_sleep_usec += _my_sleep(usec);
		if ((defer_rpc_cnt == 0) ||
		    (slurmctld_config.server_thread_count <= max_rpc_cnt))
			break;
		verbose("migration: continuing to yield locks, %d RPCs pending",
			slurmctld_config.server_thread_count);
	}
	lock_slurmctld(all_locks);
	slurm_mutex_lock(&config_lock);
	if (config_flag)
		load_config = true;
	slurm_mutex_unlock(&config_lock);

	if ((last_job_update  == job_update)  &&
	    (last_node_update == node_update) &&
	    (last_part_update == part_update) &&
	    (! stop_migration) && (! load_config))
		return 0;
	else
		return 1;
}

/* Test if this job still has access to the specified partition. The job's
 * available partitions may have changed when locks were released */
/*
static bool _job_part_valid(struct job_record *job_ptr,
			    struct part_record *part_ptr)
{
	struct part_record *avail_part_ptr;
	ListIterator part_iterator;
	bool rc = false;

	if (job_ptr->part_ptr_list) {
		part_iterator = list_iterator_create(job_ptr->part_ptr_list);
		while ((avail_part_ptr = (struct part_record *)
				list_next(part_iterator))) {
			if (avail_part_ptr == part_ptr) {
				rc = true;
				break;
			}
		}
		list_iterator_destroy(part_iterator);
	} else if (job_ptr->part_ptr == part_ptr) {
		rc = true;
	}

	return rc;
}
*/

/*
	Restrictions for the job migration.

	Here, the main objective is to concentrate multi-task jobs on as little nodes as
	possible. This way we minimize communication and reduce the execution time.

	So far, limitations are:
	- only perform job migration if node shraing (slots or cores) is enabled. If node
	sharing is not available, and thus jobs received full nodes, then migration makes no sense
	- only migrate multi-task jobs, not serial ones
	- do not migrate jobs where the user has specified number of nodes and tasks per node,
	only jobs where "number of tasks" was specified
	- do not migrate jobs where the node was a requirement
	- do not migrate jobs where user demanded exclusive use ofe the node
	- MANUEL Still not decided what to do when user asked for contiguous nodes
*/

static bool _can_be_migrated(struct job_record *job_ptr){
		if (job_ptr->cpu_cnt <2){
			debug ("Job %u is not multi-core, not migrating this job", job_ptr->job_id);
			return false;
		}
		if (job_ptr->details->ntasks_per_node > 0) {
			debug ("User has specified tasks per node for job %u, not migrating this job",job_ptr->job_id);
			return false;
		}
		if (job_ptr->details->min_nodes > 1) {
			debug ("User has specified number of nodes for job %u, not migrating this job", job_ptr->job_id);
			return false;
		}
		if (job_ptr->details->req_nodes != NULL ) {
			debug ("User has specified required nodes for job %u, not migrating this job", job_ptr->job_id);
			return false;
		}
		if (job_ptr->details->whole_node == 1 ) {
			debug ("User has specified whole node for job %u, not migrating this job", job_ptr->job_id);
			return false;
		}
		if (job_ptr->details->contiguous >0 ) {
			debug ("User has specified contiguous for job %u, I don't know what to do, not migrating this job", job_ptr->job_id);
			return false;
		}
		return true;

}


/*
we want to see whether all the tasks are concentrated in the minnimal possible ammount of nodes,
or there could be better solutions.

Complete information about node_bitmap is can be found in /src/common/job_resources.h

* Sample layout of core_bitmap:
*   |               Node_0              |               Node_1              |
*   |      Sock_0     |      Sock_1     |      Sock_0     |      Sock_1     |
*   | Core_0 | Core_1 | Core_0 | Core_1 | Core_0 | Core_1 | Core_0 | Core_1 |
*   | Bit_0  | Bit_1  | Bit_2  | Bit_3  | Bit_4  | Bit_5  | Bit_6  | Bit_7  |
*/

/*
			////////////////////////////////////////////////////////////////
			///////////////////NODE INFORMATION ////////////////////////////

			/////ALL NODES
			avail_node_bitmap: bitmap of available nodes, state not DOWN, DRAIN or FAILING
			idle_node_bitmap;	bitmap of idle nodes
			share_node_bitmap: Si se puede compartir con mas trabajos o no. TODO MANUEL: ver valores a ver como funciona.
			Se puede ver con algo como

			debug ("content of avail_node_bitmap, with lenght %u",bit_size(avail_node_bitmap) );
			for(i=0; i < bit_size(avail_node_bitmap); i++)
				debug ("%u %u",  i, bit_test(avail_node_bitmap, i));

			//// NODES FOR EACH JOB
			job_ptr->job_resrcs->node_bitmap  : nodos que emplea ese trabajo
			job_ptr->job_resrcs->core_bitmap	: cores de esos nodos que emplea el trabajo. DEBUG: VER COMO VA ESTO EN DISTINTOS CASOS
			job_ptr->job_resrcs->core_bitmap_used: NI IDEA

			Se ven con algo como:
			for(i=0; i < bit_size(job_ptr->job_resrcs->node_bitmap); i++)
				debug ("%u %u",  i, bit_test(job_ptr->job_resrcs->node_bitmap, i));
			*/

static bool _can_be_allocated(struct job_record *job_ptr){
	int number_of_nodes = bit_set_count(job_ptr->job_resrcs->node_bitmap);
	if ( number_of_nodes < 2){
		debug ("job %u is running in a sigle node, no need to migrate", job_ptr->job_id);
		return false;
	}

	//MANUEL PROBLEMA: clusters heterogéneos con CPUs de distintos tamaños
	int avg_node_size = cluster_cpus / bit_size(avail_node_bitmap);

	//home-made CEIL implementation for positive integers
	int minimal_number_of_nodes = (int)(job_ptr->cpu_cnt / avg_node_size);
	if (minimal_number_of_nodes * avg_node_size < job_ptr->cpu_cnt)
			minimal_number_of_nodes += 1;
	if (number_of_nodes <= minimal_number_of_nodes){
		debug ("job %u is running in the minnimum posible number of nodes, no need to migrate", job_ptr->job_id);
		return false;
	}

	int available_node_count=bit_set_count(idle_node_bitmap);

	//MANUEL esto es una simplificacion problematica.
	//ejepmplo: 2 cores / nodo. Trabajo de 2 tareas. Esta corriendo 1 tarea en nodo1, 1 tarea en nodo 2. Lo ideal sería
	//migrarlo todo a nodo1, pero con esta aproximación no se puede. Hay que mejorarlo.
	if (available_node_count < minimal_number_of_nodes){
		debug ("job %u has no free space. %u nodes required, %u available ones", job_ptr->job_id, minimal_number_of_nodes, available_node_count);
		return false;
	}
	return true;
}

static bool _migration_will_save_time(struct job_record *job_ptr){

	return true;
}


static int _attempt_migration(void)
{
	DEF_TIMERS;
//	bool filter_root = false;
	List job_queue;
	job_queue_rec_t *job_queue_rec;
	//slurmdb_qos_rec_t *qos_ptr = NULL;
//	int bb, i, j, node_space_recs, mcs_select = 0;
	struct job_record *job_ptr;
//	struct part_record *part_ptr, **bf_part_ptr = NULL;
//	uint32_t end_time, end_reserve, deadline_time_limit;
	//uint32_t time_limit, comp_time_limit, orig_time_limit, part_time_limit;
	//uint32_t min_nodes, max_nodes, req_nodes;
	bitstr_t  *avail_bitmap = NULL; //*active_bitmap = NULL,
	bitstr_t *exc_core_bitmap = NULL, *resv_bitmap = NULL;
	time_t now, sched_start; // later_start, start_res, resv_end, window_end;
	time_t orig_sched_start = (time_t) 0; //, orig_start_time
//	node_space_map_t *node_space;
	struct timeval bf_time1, bf_time2;
	int rc = 0;
	int job_test_count = 0, test_time_count = 0; //, pend_time;
	uint32_t *uid = NULL,  *bf_part_jobs = NULL; //nuser = 0, bf_parts = 0,
	uint16_t *njobs = NULL;
//	bool already_counted;
	//uint32_t reject_array_job_id = 0;
//	struct part_record *reject_array_part = NULL;
//	uint32_t job_start_cnt = 0, start_time;
	time_t config_update = slurmctld_conf.last_update;
	time_t part_update = last_part_update;
	struct timeval start_tv;
//	uint32_t test_array_job_id = 0;
//	uint32_t test_array_count = 0;
	//uint32_t acct_max_nodes, wait_reason = 0, job_no_reserve;
//	bool resv_overlap = false;
//	uint8_t save_share_res, save_whole_node;
//	int test_fini;

	bf_sleep_usec = 0;
	#ifdef HAVE_ALPS_CRAY
		/*
		* Run a Basil Inventory immediately before setting up the schedule
		* plan, to avoid race conditions caused by ALPS node state change.
		* Needs to be done with the node-state lock taken.
		*/
		START_TIMER;
		if (select_g_update_block(NULL)) {
			debug4("migration: not scheduling due to ALPS");
			return SLURM_SUCCESS;
		}
		END_TIMER;
		if (debug_flags & DEBUG_FLAG_MIGRATION)
			info("migration: ALPS inventory completed, %s", TIME_STR);

		/* The Basil inventory can take a long time to complete. Process
		* pending RPCs before starting the migration scheduling logic */
		_yield_locks(1000000);
	#endif
	(void) bb_g_load_state(false);

	START_TIMER;
	if (debug_flags & DEBUG_FLAG_MIGRATION)
		info("\n\n\n\n\nmigration: beginning");
	else
		debug("\n\n\n\n\nmigration: beginning");
	sched_start = orig_sched_start = now = time(NULL);
	gettimeofday(&start_tv, NULL);


/*
	if (slurm_get_root_filter())
		filter_root = true;
*/

	//MANUEL
	job_queue = _build_running_job_queue();
	job_test_count = list_count(job_queue);
	if (job_test_count == 0) {
		if (debug_flags & DEBUG_FLAG_MIGRATION)
			info("MANUEL migration: no running jobs");
		else
			debug("MANUEL migration: no running jobs");
		FREE_NULL_LIST(job_queue);
		return 0;
	}

	if (slurmctld_diag_stats.mg_active ==1){
		debug ("Migration is already being executed, exiting.");
		return 0;
	}
	slurmctld_diag_stats.mg_active = 1;
	sort_job_queue(job_queue);

	while (1) {
//		debug ("MANUEL 1");
		job_queue_rec = (job_queue_rec_t *) list_pop(job_queue);
		if (!job_queue_rec) {
			if (debug_flags & DEBUG_FLAG_MIGRATION)
			info("migration: reached end of job queue");
			break;
		}
//		debug ("MANUEL 2");
		//MANUEL: esto CREO que significa que si han dado a "apagar",  cieerres todo y a casa.
		if (slurmctld_config.shutdown_time ||
			(difftime(time(NULL),orig_sched_start)>=migration_interval)){
				xfree(job_queue_rec);
				break;
			}
//			debug ("MANUEL 3");
		//Esto son cosas de la configuración que intuyo que es mejor no tocar
		if (((defer_rpc_cnt > 0) &&
		(slurmctld_config.server_thread_count >= defer_rpc_cnt)) ||
		(_delta_tv(&start_tv) >= sched_timeout)) {
			if (debug_flags & DEBUG_FLAG_MIGRATION) {
				END_TIMER;
				info("migration: yielding locks after testing "
				"%u(%d) jobs, %s",
				slurmctld_diag_stats.bf_last_depth,
				job_test_count, TIME_STR);
			}
			if ((_yield_locks(yield_sleep) && !migration_continue) ||
			(slurmctld_conf.last_update != config_update) ||
			(last_part_update != part_update)) {
				if (debug_flags & DEBUG_FLAG_MIGRATION) {
					info("migration: system state changed, "
					"breaking out after testing "
					"%u(%d) jobs",
					slurmctld_diag_stats.bf_last_depth,
					job_test_count);
				}
				rc = 1;
				xfree(job_queue_rec);
				break;
			}
			/* Reset migration scheduling timers, resume testing */
			sched_start = time(NULL);
			gettimeofday(&start_tv, NULL);
			job_test_count = 0;
			test_time_count = 0;
			START_TIMER;
		}
//		debug ("MANUEL 4");
		//ESTE ES EL TRABAJO EN EJECUCION QUE PODEMOS MIGRAR
		//vamos a ver que este todo bien
		job_ptr  = job_queue_rec->job_ptr;
//		debug ("MANUEL 5");
		if (!job_ptr)	/* All task array elements started */
			continue;
//			debug ("MANUEL 6");
		if (!IS_JOB_RUNNING(job_ptr))	/* Something happened while getting here */
			continue;
//		debug ("MANUEL 7");

/*
		if (!_can_be_migrated(job_ptr))
			continue;
			debug ("MANUEL 7.1");

		if (!_can_be_allocated(job_ptr))
			continue;
		debug ("MANUEL 7.2");

*/


		if (!_migration_will_save_time(job_ptr))
			continue;
//		debug ("MANUEL 8");


		if (job_ptr->details->req_nodes != NULL ) {
			debug ("User has specified required nodes for job %u, not migrating this job", job_ptr->job_id);
			continue;
		}

		//////////////////////////////
		//////////////////////////////
		/////////JOB MIGRATION

//MANUEL: hay que poner los nodos aqui. Pensar formato y demás
  	slurm_checkpoint_migrate (job_ptr->job_id, NO_VAL, "slurm-compute3");


		debug ("End of migration for job %u", job_ptr->job_id);
	} //while recorrer todos los trabajos running


//		debug ("MANUEL CLEAN 1");
		//DESDE AQUI, LIMPIEZA
		xfree(bf_part_jobs);
		//xfree(bf_part_ptr);
		xfree(uid);
		xfree(njobs);
		FREE_NULL_BITMAP(avail_bitmap);
		FREE_NULL_BITMAP(exc_core_bitmap);
		FREE_NULL_BITMAP(resv_bitmap);

		//MANUEL esto lo mismo hay que liberarlo! Lo he quitado de momento
//		debug ("MANUEL CLEAN 2");
		/*
		for (i=0; ; ) {
			FREE_NULL_BITMAP(node_space[i].avail_bitmap);
			if ((i = node_space[i].next) == 0)
			break;
		}
*/
//		debug ("MANUEL CLEAN 3");
		//MANUEL esto lo mismo hay que liberarlo! Lo he quitado de momento

	//	xfree(node_space);
//		debug ("MANUEL CLEAN 3.1");

		FREE_NULL_LIST(job_queue);
//		debug ("MANUEL CLEAN 3.2");
		gettimeofday(&bf_time2, NULL);
		_do_diag_stats(&bf_time1, &bf_time2);
//		debug ("MANUEL CLEAN 3.3");


		if (debug_flags & DEBUG_FLAG_MIGRATION) {
			END_TIMER;
			info("migration: completed testing %u(%d) jobs, %s",
			slurmctld_diag_stats.bf_last_depth,
			job_test_count, TIME_STR);
		}

//		debug ("MANUEL CLEAN 4");
		if (slurmctld_config.server_thread_count >= 150) {
			info("migration: %d pending RPCs at cycle end, consider "
			"configuring max_rpc_cnt",
			slurmctld_config.server_thread_count);
		}
//		debug ("MANUEL CLEAN 5, just before exit");
		return rc;
	}

/* Try to start the job on any non-reserved nodes */


/* MANUEL: ESTO NO SE USABA, FUERA
static int _start_job(struct job_record *job_ptr, bitstr_t *resv_bitmap)
{
	int rc;
	bitstr_t *orig_exc_nodes = NULL;
	bool is_job_array_head = false;
	static uint32_t fail_jobid = 0;

	if (job_ptr->details->exc_node_bitmap) {
		orig_exc_nodes = bit_copy(job_ptr->details->exc_node_bitmap);
		bit_or(job_ptr->details->exc_node_bitmap, resv_bitmap);
	} else
		job_ptr->details->exc_node_bitmap = bit_copy(resv_bitmap);
	if (job_ptr->array_recs)
		is_job_array_head = true;
	rc = select_nodes(job_ptr, false, NULL, NULL, NULL);
	if (is_job_array_head && job_ptr->details) {
		struct job_record *base_job_ptr;
		base_job_ptr = find_job_record(job_ptr->array_job_id);
		if (base_job_ptr && base_job_ptr != job_ptr
				 && base_job_ptr->array_recs) {
			FREE_NULL_BITMAP(
					base_job_ptr->details->exc_node_bitmap);
			if (orig_exc_nodes)
				base_job_ptr->details->exc_node_bitmap =
					bit_copy(orig_exc_nodes);
		}
	}
	if (job_ptr->details) {
		FREE_NULL_BITMAP(job_ptr->details->exc_node_bitmap);
		job_ptr->details->exc_node_bitmap = orig_exc_nodes;
	} else
		FREE_NULL_BITMAP(orig_exc_nodes);
	if (rc == SLURM_SUCCESS) {
		last_job_update = time(NULL);
		if (job_ptr->array_task_id == NO_VAL) {
			info("migration: Started JobId=%u in %s on %s",
			     job_ptr->job_id, job_ptr->part_ptr->name,
			     job_ptr->nodes);
		} else {
			info("migration: Started JobId=%u_%u (%u) in %s on %s",
			     job_ptr->array_job_id, job_ptr->array_task_id,
			     job_ptr->job_id, job_ptr->part_ptr->name,
			     job_ptr->nodes);
		}
		power_g_job_start(job_ptr);
		if (job_ptr->batch_flag == 0)
			srun_allocate(job_ptr->job_id);
		else if ((job_ptr->details == NULL) ||
			 (job_ptr->details->prolog_running == 0))
			launch_job(job_ptr);
		slurmctld_diag_stats.migrated_jobs++;
		slurmctld_diag_stats.last_migrated_jobs++;
		if (debug_flags & DEBUG_FLAG_MIGRATION) {
			info("migration: Jobs migrated since boot: %u",
			     slurmctld_diag_stats.migrated_jobs);
		}
	} else if ((job_ptr->job_id != fail_jobid) &&
		   (rc != ESLURM_ACCOUNTING_POLICY)) {
		char *node_list;
		bit_not(resv_bitmap);
		node_list = bitmap2node_name(resv_bitmap);
		verbose("migration: Failed to start JobId=%u with %s avail: %s",
			job_ptr->job_id, node_list, slurm_strerror(rc));
		xfree(node_list);
		fail_jobid = job_ptr->job_id;
	} else {
		debug3("migration: Failed to start JobId=%u: %s",
		       job_ptr->job_id, slurm_strerror(rc));
	}

	return rc;
}

*/
/* Reset a job's time limit (and end_time) as high as possible
 *	within the range job_ptr->time_min and job_ptr->time_limit.
 *	Avoid using resources reserved for pending jobs or in resource
 *	reservations */

/* MANUEL: ESTO NO SE USABA, FUERA
static void _reset_job_time_limit(struct job_record *job_ptr, time_t now,
				  node_space_map_t *node_space)
{
	int32_t j, resv_delay;
	uint32_t orig_time_limit = job_ptr->time_limit;
	uint32_t new_time_limit;

	for (j=0; ; ) {
		if ((node_space[j].begin_time != now) &&
		    (node_space[j].begin_time < job_ptr->end_time) &&
		    (!bit_super_set(job_ptr->node_bitmap,
				    node_space[j].avail_bitmap))) {
			resv_delay = difftime(node_space[j].begin_time, now);
			resv_delay /= 60;
			if (resv_delay < job_ptr->time_limit)
				job_ptr->time_limit = resv_delay;
		}
		if ((j = node_space[j].next) == 0)
			break;
	}
	new_time_limit = MAX(job_ptr->time_min, job_ptr->time_limit);
	acct_policy_alter_job(job_ptr, new_time_limit);
	job_ptr->time_limit = new_time_limit;
	job_ptr->end_time = job_ptr->start_time + (job_ptr->time_limit * 60);

	job_time_adj_resv(job_ptr);

	if (orig_time_limit != job_ptr->time_limit) {
		info("migration: job %u time limit changed from %u to %u",
		     job_ptr->job_id, orig_time_limit, job_ptr->time_limit);
	}
}
*/


/* Report if any changes occurred to job, node or partition information */
static bool _more_work (time_t last_migration_time)
{
	bool rc = false;

	slurm_mutex_lock( &thread_flag_mutex );
	if ( (last_job_update  >= last_migration_time ) ||
	     (last_node_update >= last_migration_time ) ||
	     (last_part_update >= last_migration_time ) ) {
		rc = true;
	}
	slurm_mutex_unlock( &thread_flag_mutex );
	return rc;
}

/* Create a reservation for a job in the future */
//MANUEL: ESTO PUEDE SER MUY NECESARIO PARA LA MIGRACION!!!!
static void _add_reservation(uint32_t start_time, uint32_t end_reserve,
			     bitstr_t *res_bitmap,
			     node_space_map_t *node_space,
			     int *node_space_recs)
{
	bool placed = false;
	int i, j;

#if 0
	info("add job start:%u end:%u", start_time, end_reserve);
	for (j = 0; ; ) {
		info("node start:%u end:%u",
		     (uint32_t) node_space[j].begin_time,
		     (uint32_t) node_space[j].end_time);
		if ((j = node_space[j].next) == 0)
			break;
	}
#endif

	start_time = MAX(start_time, node_space[0].begin_time);
	for (j = 0; ; ) {
		if (node_space[j].end_time > start_time) {
			/* insert start entry record */
			i = *node_space_recs;
			node_space[i].begin_time = start_time;
			node_space[i].end_time = node_space[j].end_time;
			node_space[j].end_time = start_time;
			node_space[i].avail_bitmap =
				bit_copy(node_space[j].avail_bitmap);
			node_space[i].next = node_space[j].next;
			node_space[j].next = i;
			(*node_space_recs)++;
			placed = true;
		}
		if (node_space[j].end_time == start_time) {
			/* no need to insert new start entry record */
			placed = true;
		}
		if (placed == true) {
			while ((j = node_space[j].next)) {
				if (end_reserve < node_space[j].end_time) {
					/* insert end entry record */
					i = *node_space_recs;
					node_space[i].begin_time = end_reserve;
					node_space[i].end_time = node_space[j].
								 end_time;
					node_space[j].end_time = end_reserve;
					node_space[i].avail_bitmap =
						bit_copy(node_space[j].
							 avail_bitmap);
					node_space[i].next = node_space[j].next;
					node_space[j].next = i;
					(*node_space_recs)++;
					break;
				}
				if (end_reserve == node_space[j].end_time) {
					break;
				}
			}
			break;
		}
		if ((j = node_space[j].next) == 0)
			break;
	}

	for (j = 0; ; ) {
		if ((node_space[j].begin_time >= start_time) &&
		    (node_space[j].end_time <= end_reserve))
			bit_and(node_space[j].avail_bitmap, res_bitmap);
		if ((node_space[j].begin_time >= end_reserve) ||
		    ((j = node_space[j].next) == 0))
			break;
	}

	/* Drop records with identical bitmaps (up to one record).
	 * This can significantly improve performance of the migration tests. */
	for (i = 0; ; ) {
		if ((j = node_space[i].next) == 0)
			break;
		if (!bit_equal(node_space[i].avail_bitmap,
			       node_space[j].avail_bitmap)) {
			i = j;
			continue;
		}
		node_space[i].end_time = node_space[j].end_time;
		node_space[i].next = node_space[j].next;
		FREE_NULL_BITMAP(node_space[j].avail_bitmap);
		break;
	}
}

/*
 * Determine if the resource specification for a new job overlaps with a
 *	reservation that the migration scheduler has made for a job to be
 *	started in the future.
 * IN use_bitmap - nodes to be allocated
 * IN start_time - start time of job
 * IN end_reserve - end time of job
 */

 /* MANUEL: ESTO NO SE USABA, FUERA
static bool _test_resv_overlap(node_space_map_t *node_space,
			       bitstr_t *use_bitmap, uint32_t start_time,
			       uint32_t end_reserve)
{
	bool overlap = false;
	int j;

	for (j=0; ; ) {
		if ((node_space[j].end_time   > start_time) &&
		    (node_space[j].begin_time < end_reserve) &&
		    (!bit_super_set(use_bitmap, node_space[j].avail_bitmap))) {
			overlap = true;
			break;
		}
		if ((j = node_space[j].next) == 0)
			break;
	}
	return overlap;
}
*/


static void _job_queue_rec_del(void *x)
{
	xfree(x);
}
/*
 * _build_running_job_queue - build (non-priority ordered) list of running jobs
 * RET the job queue
 * NOTE: the caller must call FREE_NULL_LIST() on RET value to free memory
 */

extern List _build_running_job_queue()
{
	List job_queue;
	ListIterator job_iterator;
	struct job_record *job_ptr = NULL;
	struct timeval start_tv = {0, 0};

	(void) _delta_tv(&start_tv);
	job_queue = list_create(_job_queue_rec_del);

	/* Create individual job records for job arrays that need burst buffer
	 * staging */
	job_iterator = list_iterator_create(job_list);
	while ((job_ptr = (struct job_record *) list_next(job_iterator))) {
		if (IS_JOB_RUNNING(job_ptr)) {
			job_queue_rec_t *job_queue_rec;
			job_queue_rec = xmalloc(sizeof(job_queue_rec_t));
			job_queue_rec->array_task_id = job_ptr->array_task_id;
			job_queue_rec->job_id   = job_ptr->job_id;
			job_queue_rec->job_ptr  = job_ptr;
			job_queue_rec->priority = job_ptr->priority; //MANUEL: estyo es importante, si aqui damos una prioridad distinta eso quizá afecte a la migración
			list_append(job_queue, job_queue_rec);
		}
	}
	list_iterator_destroy(job_iterator);
	return job_queue;
}
