
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h>

#include "slurm/slurm.h"
#include "src/common/checkpoint.h"
#include "src/common/slurm_protocol_api.h"
#include "src/plugins/slurmctld/migration/migration.h"


const char plugin_name[]       	= "Migration";
const char plugin_type[]				= "slurmctld_plugstack/migration";

const uint32_t plugin_version	= SLURM_VERSION_NUMBER;

/*
 * init() is called when the plugin is loaded, before any other functions
 * are called.  Put global initialization here.
 */
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



/*
 * _migrate_op - Migration operation. Just like checkpoint, but allows to specify destination nodes
 * IN job_id    - job on which to perform operation
 * IN step_id   - job step on which to perform operation
 * IN destination_nodes - destination nodes
 * IN image_dir - directory used to get/put checkpoint images
 * RET 0 or a slurm error code
 */

extern int slurm_checkpoint_migrate (uint32_t job_id, uint32_t step_id, char *destination_nodes)
{

	int error = 0;

	/*VERIFICATION OF INPUT DATA */
	job_info_msg_t * job_ptr = NULL;
	uint16_t show_flags = 0;
	if (slurm_load_job (&job_ptr, job_id, show_flags) != SLURM_SUCCESS ){
		slurm_perror ("Specified ID does not correspond to an existing Slurm task\n");
		return (EMIGRATION_NOT_JOB);
	}

	slurm_job_info_t job_info = job_ptr->job_array[job_ptr->record_count-1];
	if (job_info.job_state  != JOB_RUNNING) {
		slurm_perror ("Jobs must be RUNNING to be migrated.\n");
		return (EMIGRATION_JOB_ERROR);
	}

	time_t start_time;
	if (( error = slurm_checkpoint_able( job_id, step_id, &start_time)) != SLURM_SUCCESS){
		slurm_perror ("Job is not checkpointable\n");
		return(EMIGRATION_JOB_ERROR);
	}

	//TODO manuel. Aqui split del destiantion_nodes para trabajos paralelos
	node_info_msg_t *node_access = NULL;
	if (( error = slurm_load_node_single(&node_access, destination_nodes, show_flags)) != 0) {
		slurm_perror ("Specified node does not exist.\n");
		return(EMIGRATION_DEST_ERROR);
	}

	node_info_t node = node_access->node_array[0];
	if (node.node_state !=	NODE_STATE_IDLE) {
		printf ("Node should be iddle and ready to be used.\n");
		return(EMIGRATION_DEST_ERROR);
	}

	/*checkpoint */
	char* checkpoint_directory = slurm_get_checkpoint_dir();

	if (( error = slurm_checkpoint_create (job_id, step_id, 0,checkpoint_directory )) != 0){
		slurm_perror ("there was an error calling slurm_checkpoint_vacate.");
		return(EMIGRATION_ERROR);
	 }

	 if (( error = slurm_terminate_job_step(job_id, 0)) != 0){
		 slurm_perror ("there was an error calling slurm_terminate_job_step.");
		 return(EMIGRATION_ERROR);
		}

	//wait until job has been purged
	//MANUEL. this can take some time. Should be performed in a different thread or something?
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

		/*restart checkpoint */
	job_info.req_nodes = destination_nodes;
	if (slurm_checkpoint_restart(job_id, step_id, 0,  checkpoint_directory) != 0) {
		slurm_perror("Error restarting job\n");
		return(EMIGRATION_ERROR);
		}
	return (EMIGRATION_SUCCESS);
}
