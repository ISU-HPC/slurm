
#if HAVE_CONFIG_H
#  include "config.h"
#endif


#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

#include "slurm/slurm.h"
#include "src/common/checkpoint.h"
#include "src/common/slurm_protocol_api.h"
#include "src/plugins/slurmctld/job_migration/job_migration.h"


/*

const char plugin_name[]       	= "Job_Migration";
const char plugin_type[]				= "slurmctld_plugstack/job_migration";

const uint32_t plugin_version	= SLURM_VERSION_NUMBER;
*/

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */

 /*
extern int init ( void )
{
	info("slurmctld_plugstack/migration init");
	return SLURM_SUCCESS;
}


extern int fini ( void )
{
	info("slurmctld_plugstack/migration fini");
	return SLURM_SUCCESS;
}
*/


static int _job_info_to_job_desc (job_info_t job_info, job_desc_msg_t *job_desc_msg);

static int _print_existing_jobs (void){

	job_info_msg_t * job_buffer_ptr;
	uint16_t show_flags = 0;
	time_t update_time = 0;
	int counter = 0;
	FILE* fout = stdout;
	printf ("PRINT EXSITING JOBS\n");
	printf (	"loading jobs\n");
	slurm_load_jobs (update_time, &job_buffer_ptr, show_flags);

	printf ("	jobs loaded\n");
	printf ("	number of elements: %d \n", job_buffer_ptr->record_count);

	while (counter < job_buffer_ptr->record_count){
		printf ("	printing job %d\n", counter);

		slurm_print_job_info (fout, &job_buffer_ptr->job_array[counter], 0);
		counter +=1;
		}
	slurm_free_job_info_msg(job_buffer_ptr);

	return 0;

}


extern int slurm_checkpoint_migrate (uint32_t job_id, uint32_t step_id, char *destination_nodes)
{

	int error = 0;

	/*VERIFICATION OF INPUT DATA */
	job_info_msg_t * job_ptr;
	uint16_t show_flags = 0;
	printf("id is %d\n",job_id );

	if ((error = slurm_load_job (&job_ptr, job_id, show_flags)) != SLURM_SUCCESS ){
		printf("Error is %d\n", error);
		slurm_perror ("Specified ID does not correspond to an existing Slurm task");
	//	_print_existing_jobs();
		return (EMIGRATION_NOT_JOB);
	}

	slurm_job_info_t job_info = job_ptr->job_array[job_ptr->record_count-1];
	if (job_info.job_state  != JOB_RUNNING) {
		slurm_perror ("Jobs must be RUNNING to be migrated");
		return (EMIGRATION_JOB_ERROR);
	}
	printf("Job %d is running, great\n", job_id );

	time_t start_time;

	//TODO aqui el segundo parametro, NO_VAl, deberia ser step_id. Por algun motivo casca asi que pongo esto.
	if (( error = slurm_checkpoint_able( job_id, NO_VAL, &start_time)) != SLURM_SUCCESS){
		slurm_perror ("Job is not checkpointable");
		return(EMIGRATION_JOB_ERROR);
	}

	printf("Job %d is checkpointable, great\n", job_id );




/*

	if (slurm_allocate_resources(job_desc_msg, &slurm_alloc_msg_ptr)!=0) {
		slurm_perror ((char *)"slurm_allocate_resources_blocking error");
		exit (EMIGRATION_DEST_ERROR);
	}

	if (slurm_alloc_msg_ptr->node_cnt <=0) {
		slurm_perror ((char *)"could not allocate resources for the job");
		exit (EMIGRATION_DEST_ERROR);
	}

	printf("Resources for job %d have been granted\n", job_id );
	slurm_free_resource_allocation_response_msg(slurm_alloc_msg_ptr);


*/
	/*checkpoint */


/*
	job_desc_msg_t *job_desc_msg;
	slurm_init_job_desc_msg(job_desc_msg);
	job_desc_msg->job_id = job_id;
	job_desc_msg->req_nodes = destination_nodes;


	if (slurm_update_job(job_desc_msg) != 0) {
		slurm_perror("Error setting new host for job");
		return(EMIGRATION_ERROR);
		}
*/

	//MANUEL:  ESTO SE SUPONE QUE FUNCIONABA!!!
	job_info.req_nodes = destination_nodes;
	job_info.nodes = destination_nodes;
	job_info.batch_host = destination_nodes;
	printf("Destination nodes: %s\n", destination_nodes);



	char* checkpoint_directory = slurm_get_checkpoint_dir();

	if (( error = slurm_checkpoint_vacate (job_id, step_id, 0,checkpoint_directory )) != 0){
		slurm_perror ("there was an error calling slurm_checkpoint_create.");
		return(EMIGRATION_ERROR);
	 }
	 printf("Checkpoint has been created! \n");


	//TODO MANUEL. this can take some time. Should be performed in a different thread or something?
	while (job_info.job_state  == JOB_RUNNING){
		if (slurm_load_job (&job_ptr, job_id, show_flags) != SLURM_SUCCESS ){
			slurm_perror ("there was an error loading job info.");
			return(EMIGRATION_ERROR);
		 }
		sleep(1);
		job_info = job_ptr->job_array[job_ptr->record_count-1];
	}

	if (job_info.job_state != JOB_COMPLETE){
		slurm_perror("Job is a wrong status for checkpoint, aborting\n");
		return(EMIGRATION_ERROR);
		}

		printf("Job is finally finallized. Now we'll wait for it to be deleted from the system (this might take a while)\n");

		while (slurm_load_job (&job_ptr, job_id, show_flags) == SLURM_SUCCESS ){
			printf ("still exists LOL\n");
			sleep(1);
		}
		printf("it should be deleted by now\n");


		/*restart checkpoint */
	if (slurm_checkpoint_restart(job_id, step_id, 0,  checkpoint_directory) != 0) {
		slurm_perror("Error restarting job\n");
		return(EMIGRATION_ERROR);
		}

	printf("Job %d has been restarted! \n", job_id);
	printf ("let's check in a few seconds\n");
	sleep(10);
	_print_existing_jobs();


	return (EMIGRATION_SUCCESS);
}



// comment with //: do not exist in job_info
// comment with ////: disabled
static int _job_info_to_job_desc (job_info_t job_info, job_desc_msg_t *job_desc_msg){
	job_desc_msg->account= job_info.account ;
//  job_desc_msg.acctg_freq= job_info. ;
  job_desc_msg->alloc_node= job_info.alloc_node ;
//  job_desc_msg.alloc_resp_port= job_info. ;
////  job_desc_msg->alloc_sid= job_info.alloc_sid ;
//  job_desc_msg.argc= job_info. ;
//  job_desc_msg.argv= job_info. ;
//  job_desc_msg.array_inx= job_info. ;
//  job_desc_msg.array_bitmap= job_info. ;
//  job_desc_msg.begin_time= job_info. ;
//  job_desc_msg->bitflags= job_info.bitflags ;
////  job_desc_msg->burst_buffer= job_info.burst_buffer ;
//  job_desc_msg.ckpt_interval= job_info. ;
//  job_desc_msg.ckpt_dir= job_info. ;
//  job_desc_msg.clusters= job_info. ;
  job_desc_msg->comment= job_info.comment ;
  job_desc_msg->contiguous= job_info.contiguous ;
////  job_desc_msg->core_spec= job_info.core_spec ;
//  job_desc_msg.cpu_bind= job_info. ;
//  job_desc_msg.cpu_bind_type= job_info. ;
////  job_desc_msg->cpu_freq_min= job_info.cpu_freq_min ;
  ////job_desc_msg->cpu_freq_max= job_info.cpu_freq_max ;
  ////job_desc_msg->cpu_freq_gov= job_info.cpu_freq_gov ;
  job_desc_msg->dependency= job_info.dependency ;
  job_desc_msg->end_time= job_info.end_time ;
//  job_desc_msg.environment= job_info. ;
//  job_desc_msg.env_size= job_info. ;
  job_desc_msg->exc_nodes= job_info.exc_nodes ;
  job_desc_msg->features= job_info.features ;
  job_desc_msg->gres= job_info.gres ;
  job_desc_msg->group_id= job_info.group_id ;
// job_desc_msg.immediate= job_info. ;
////  job_desc_msg->job_id= job_info.job_id ;
//  job_desc_msg.job_id_str= job_info. ;
//  job_desc_msg.kill_on_node_fail= job_info. ;
  job_desc_msg->licenses= job_info.licenses ;
//  job_desc_msg.mail_type= job_info. ;
//  job_desc_msg.mail_user= job_info. ;
//  job_desc_msg.mem_bind= job_info. ;
//  job_desc_msg.mem_bind_type= job_info. ;
  job_desc_msg->name= job_info.name ;
  job_desc_msg->network= job_info.network ;
  job_desc_msg->nice= job_info.nice ;
  job_desc_msg->num_tasks= job_info.num_tasks ;
//  job_desc_msg.open_mode= job_info. ;
//  job_desc_msg.other_port= job_info. ;
//  job_desc_msg.overcommit= job_info. ;
  job_desc_msg->partition= job_info.partition ;
//  job_desc_msg.plane_size= job_info. ;
  ////job_desc_msg->power_flags= job_info.power_flags ;
  job_desc_msg->priority= job_info.priority ;
  job_desc_msg->profile= job_info.profile ;
  job_desc_msg->qos= job_info.qos ;
  job_desc_msg->reboot= job_info.reboot ;
//  job_desc_msg.resp_host= job_info. ;
  job_desc_msg->req_nodes= job_info.req_nodes ;
////  job_desc_msg->requeue= job_info.requeue ;
//  job_desc_msg.reservation= job_info. ;
//  job_desc_msg.script= job_info. ;
//  job_desc_msg.shared= job_info. ;
//  job_desc_msg.spank_job_env= job_info. ;
//  job_desc_msg.spank_job_env_size= job_info. ;
//  job_desc_msg.task_dist= job_info. ;
  job_desc_msg->time_limit= job_info.time_limit ;
	job_desc_msg->time_min = job_info.time_min;
	job_desc_msg->user_id = job_info.user_id;
//  job_desc_msg.wait_all_nodes= job_info. ;
//  job_desc_msg.warn_flags= job_info. ;
//  job_desc_msg.warn_signal= job_info. ;
//  job_desc_msg.warn_time= job_info. ;
//  job_desc_msg.work_dir= job_info. ;
  job_desc_msg->cpus_per_task= job_info.cpus_per_task ;
//  job_desc_msg.min_cpus= job_info. ;
  job_desc_msg->max_cpus= job_info.max_cpus ;
//  job_desc_msg.min_nodes= job_info. ;
  job_desc_msg->max_nodes= job_info.max_nodes ;
  job_desc_msg->boards_per_node= job_info.boards_per_node ;
  job_desc_msg->sockets_per_board= job_info.sockets_per_board ;
  job_desc_msg->sockets_per_node= job_info.sockets_per_node ;
  job_desc_msg->cores_per_socket= job_info.cores_per_socket ;
  job_desc_msg->threads_per_core= job_info.threads_per_core ;
  job_desc_msg->ntasks_per_node= job_info.ntasks_per_node ;
  job_desc_msg->ntasks_per_socket= job_info.ntasks_per_socket ;
  job_desc_msg->ntasks_per_core= job_info.ntasks_per_core ;
  job_desc_msg->ntasks_per_board= job_info.ntasks_per_board ;
  job_desc_msg->pn_min_cpus= job_info.pn_min_cpus ;
  job_desc_msg->pn_min_memory= job_info.pn_min_memory ;
  job_desc_msg->pn_min_tmp_disk= job_info.pn_min_tmp_disk ;
  job_desc_msg->req_switch= job_info.req_switch ;
  job_desc_msg->select_jobinfo= job_info.select_jobinfo ;
//  job_desc_msg.std_err= job_info. ;
//  job_desc_msg.std_in= job_info. ;
//  job_desc_msg.std_out= job_info. ;
//  job_desc_msg.tres_req_cnt= job_info.tres_req_cnt ;
  job_desc_msg->wait4switch= job_info.wait4switch ;
  job_desc_msg->wckey= job_info.wckey ;

	return SLURM_SUCCESS;
}
