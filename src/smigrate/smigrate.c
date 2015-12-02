/*****************************************************************************\
 *  srun.c - user interface to allocate resources, submit jobs, and execute
 *	parallel jobs.
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Mark Grondona <grondona@llnl.gov>, et. al.
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

#if HAVE_CONFIG_H
#  include "config.h"
#endif

#include <sys/resource.h> /* for RLIMIT_NOFILE */
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>               /* MAXPATHLEN */
#include <fcntl.h>

#include "slurm/slurm.h"

#include "src/common/cpu_frequency.h"
#include "src/common/env.h"
#include "src/common/plugstack.h"
#include "src/common/proc_args.h"
#include "src/common/read_config.h"
#include "src/common/slurm_rlimits_info.h"
#include "src/common/xstring.h"
#include "src/common/xmalloc.h"

#include "src/plugins/slurmctld/migration/migration.h"
#include "opt.h"

#define MAX_RETRIES 15

static void  _env_merge_filter(job_desc_msg_t *desc);
static int   _fill_job_desc_from_opts(job_desc_msg_t *desc);
static int   _check_cluster_specific_settings(job_desc_msg_t *desc);
static void *_get_script_buffer(const char *filename, int *size);
static char *_script_wrap(char *command_string);
static void  _set_exit_code(void);
static void  _set_prio_process_env(void);
static int   _set_rlimit_env(void);
static void  _set_spank_env(void);
static void  _set_submit_dir_env(void);
static int   _set_umask_env(void);
static int   _job_wait(uint32_t job_id);



static int   _slurm_debug_env_val (void);


int main(int argc, char *argv[])
{

  log_options_t logopt = LOG_OPTS_STDERR_ONLY;
	job_desc_msg_t desc;
	submit_response_msg_t *resp;
	char *script_name;
	void *script_body;
	int script_size = 0;
	int rc = 0, retries = 0;

	slurm_conf_init(NULL);
	log_init(xbasename(argv[0]), logopt, 0, NULL);

	_set_exit_code();
	if (spank_init_allocator() < 0) {
		error("Failed to initialize plugin stack");
		exit(error_exit);
	}

	/* Be sure to call spank_fini when sbatch exits
	 */
	if (atexit((void (*) (void)) spank_fini) < 0)
		error("Failed to register atexit handler for plugins: %m");

	script_name = process_options_first_pass(argc, argv);
	/* reinit log with new verbosity (if changed by command line) */
	if (opt.verbose || opt.quiet) {
		logopt.stderr_level += opt.verbose;
		logopt.stderr_level -= opt.quiet;
		logopt.prefix_level = 1;
		log_alter(logopt, 0, NULL);
	}


	if (process_options_second_pass(
				argc,
				argv,
				script_name ? xbasename (script_name) : "stdin",
				script_body, script_size) < 0) {
		error("sbatch parameter parsing");
		exit(error_exit);
	}


  slurm_checkpoint_migrate ( opt.jobid, opt.stepid, opt.nodes);

  return (0);
}



static int _slurm_debug_env_val (void)
{
	long int level = 0;
	const char *val;

	if ((val = getenv ("SLURM_DEBUG"))) {
		char *p;
		if ((level = strtol (val, &p, 10)) < -LOG_LEVEL_INFO)
			level = -LOG_LEVEL_INFO;
		if (p && *p != '\0')
			level = 0;
	}
	return ((int) level);
}



static void _set_exit_code(void)
{
	int i;
	char *val = getenv("SLURM_EXIT_ERROR");

	if (val) {
		i = atoi(val);
		if (i == 0)
			error("SLURM_EXIT_ERROR has zero value");
		else
			error_exit = i;
	}
}
