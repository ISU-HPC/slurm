
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

char** _str_split(char* a_str, const char a_delim);
static int _print_existing_jobs (void);
static int _job_info_to_job_desc (job_info_t job_info, job_desc_msg_t *job_desc_msg);
extern int _migrate_job(uint32_t job_id, uint32_t step_id, char *destination_nodes, char *excluded_nodes, char *partition, int shared, int spread, bool test_only);
extern int _drain_node( char *destination_nodes, char *excluded_nodes, char *drain_node, char *partition, int shared, int spread, bool test_only);

extern int slurm_checkpoint_migrate (uint32_t job_id, uint32_t step_id, char *destination_nodes, char *excluded_nodes, char *drain_node,  char *partition, int shared, int spread, bool test_only)
{

	//printf("\n\n\njob_id=%u,\nstep_id=%u,\ndestination_nodes=%s,\nexcluded_nodes=%s, \ndrain_node=%s,\nshared=%u,\nspread=%u\n\n\n",job_id, step_id, destination_nodes, excluded_nodes, drain_node, shared, spread);
	if (drain_node[0] == '\0' )
		return _migrate_job(job_id, step_id, destination_nodes, excluded_nodes, partition, shared, spread, test_only);

	else if ((drain_node[0] == '\0' ) && (job_id != 0)){
		slurm_perror ("drain-node and a job id are incompatible");
		return (EMIGRATION_JOB_ERROR);
	}
	else if (drain_node[0] != '\0' )
		return _drain_node(destination_nodes, excluded_nodes, drain_node, partition, shared, spread, test_only);

	else {
		slurm_perror ("No Job ID and no node to drain specified, exiting");
		return (EMIGRATION_JOB_ERROR);

	}

}


extern int _migrate_job(uint32_t job_id, uint32_t step_id, char *destination_nodes, char *excluded_nodes, char *partition, int shared, int spread, bool test_only){
	//	printf("\n\n\njob_id=%u,\nstep_id=%u,\ndestination_nodes=%s,\nexcluded_nodes=%s, \nshared=%u,\nspread=%u\n\n\n",job_id, step_id, destination_nodes, excluded_nodes, shared, spread);

		int error = 0;

		/*VERIFICATION OF INPUT DATA */
		job_info_msg_t * job_ptr;
		uint16_t show_flags = 0;

		if ((error = slurm_load_job (&job_ptr, job_id, show_flags)) != SLURM_SUCCESS ){
			printf("Error is %d\n", error);
			slurm_perror ("Specified ID does not correspond to an existing Slurm task");
		//	_print_existing_jobs();
			return (EMIGRATION_NOT_JOB);
		}

		time_t start_time;

		//TODO aqui el segundo parametro, NO_VAl, deberia ser step_id. Por algun motivo casca asi que pongo esto.
		if (( error = slurm_checkpoint_able( job_id, NO_VAL, &start_time)) != SLURM_SUCCESS){
			slurm_perror ("Job is not checkpointable");
			return(EMIGRATION_JOB_ERROR);
		}

		slurm_job_info_t job_info = job_ptr->job_array[job_ptr->record_count-1];
		if (job_info.job_state  != JOB_RUNNING) {
			slurm_perror ("Jobs must be RUNNING to be migrated");
			return (EMIGRATION_JOB_ERROR);
		}


		if ((job_info.req_nodes  != '\0') && (destination_nodes[0] != '\0' )){
				slurm_perror ("User specified a different destination resource on original job submission");
				return (EMIGRATION_JOB_ERROR);
			}

			//this whole copy thing is required because certain tests cannot be performed otherwise
			job_desc_msg_t job_desc_msg_test;
			slurm_init_job_desc_msg(&job_desc_msg_test);
			_job_info_to_job_desc (job_info, &job_desc_msg_test);

			job_desc_msg_test.job_id = NO_VAL;
			job_desc_msg_test.priority = NO_VAL - 1;

			if ((job_info.shared ==1) || (shared != (uint16_t)NO_VAL))
				job_desc_msg_test.shared = shared;

			if (destination_nodes[0] != '\0' )
				job_desc_msg_test.req_nodes = destination_nodes;

				if (partition[0] != '\0' )
					job_desc_msg_test.partition = partition;

			if (excluded_nodes[0] != '\0' ){
				int bufsize=999;
				char* nodesToExclude;
 				if (job_info.exc_nodes != '\0'){
					hostlist_t hl;
					hl = hostlist_create(job_info.exc_nodes);
					slurm_hostlist_push(hl, excluded_nodes);
					slurm_hostlist_uniq(hl);
					nodesToExclude = slurm_hostlist_ranged_string_malloc(hl);
					//slurm_hostlist_ranged_string(hl, bufsize, nodesToExclude);
					}
				else {
					nodesToExclude = excluded_nodes;
				//	sprintf(nodesToExclude, "%s", excluded_nodes);
					}
				job_desc_msg_test.exc_nodes = nodesToExclude;
			}

			if (spread)
				job_desc_msg_test.bitflags |= SPREAD_JOB;

			will_run_response_msg_t *will_run_resp = NULL;
			if (slurm_job_will_run2(&job_desc_msg_test, &will_run_resp) != SLURM_SUCCESS) {
				slurm_perror("Error: job will not run");
				return(EMIGRATION_ERROR);
				}
//				debug ("BEFORE TEST_ONLY");

			if (test_only)
				return (EMIGRATION_SUCCESS);

//		debug ("ABOUT TO CHECKPOINT");
	/*checkpoint*/
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
			//TODO MANUEL: comentado esto porque quiza cascaba aqui
//			return(EMIGRATION_ERROR);
		return (-1);
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

		/*Change job atributes while in queue*/

		job_desc_msg_t job_desc_msg;
		slurm_init_job_desc_msg(&job_desc_msg);

		job_desc_msg.job_id = job_info.job_id;


		if (destination_nodes[0] != '\0' )
			job_desc_msg.req_nodes = destination_nodes;

		if (excluded_nodes[0] != '\0' )
			job_desc_msg.exc_nodes = excluded_nodes;

		if (partition[0] != '\0' )
			job_desc_msg.partition = partition;


		if (shared != (uint16_t)NO_VAL)
			job_desc_msg.shared = shared;

			if (spread)
				job_desc_msg.bitflags |= SPREAD_JOB;

		char job_id_str[15];
		sprintf(job_id_str, "%d", job_info.job_id);
		slurm_top_job(job_id_str);

		if (slurm_update_job(&job_desc_msg) != 0) {
			slurm_perror("Error setting migration requirements for job");
			return(EMIGRATION_ERROR);
			}

		if ((error = slurm_load_job (&job_ptr, job_id, show_flags)) != SLURM_SUCCESS ){
			slurm_perror ("Specified ID does not correspond to an existing Slurm task");
			return (EMIGRATION_NOT_JOB);
		}


		return (EMIGRATION_SUCCESS);
}


extern int _drain_node(char *destination_nodes, char *excluded_nodes, char *drain_node, char* partition, int shared, int spread, bool test_only){

	job_info_msg_t *job_ptr = NULL;
	node_info_msg_t *node_info;
	slurm_job_info_t job_info;
	update_node_msg_t node_msg;
	slurm_job_info_t *jobs_running_in_node;
	int i, error, cont = 0;
	hostlist_t hl;
	uint32_t old_node_state;

	if (slurm_load_node_single(&node_info, drain_node, 0) != 0) {
		slurm_perror ("Could not get info from node");
		return (EMIGRATION_ERROR);
	}
	if (node_info->record_count ==0) {
		slurm_perror ("No nodes with that id were found");
		return (EMIGRATION_ERROR);
	}

	//avoid race conditions by avoiding new jobs to be asigned to the node being drained
	printf("Setting the node in DRAIN status\n");
	slurm_init_update_node_msg(&node_msg);
	node_msg.node_names=drain_node;
	old_node_state = node_msg.node_state;
	node_msg.node_state = NODE_STATE_DRAIN;
	if (slurm_update_node(&node_msg)) {
	 printf ("Could not set node %s into DRAIN status\n",drain_node );
	 return (EMIGRATION_ERROR);
	}

	//load job info
	if (slurm_load_jobs(0, &job_ptr, SHOW_DETAIL) != 0) {
		 slurm_perror ("slurm_load_jobs error");
		 return SLURM_ERROR;
	 }

 	//PRINT NODE INFO (JUST DEBUGGING)
	printf ("NODE INFO\n");
	slurm_print_node_info_msg(stdout, node_info, 0);
	printf ("ALL JOBS INFO\n");
	slurm_print_job_info_msg(stdout, job_ptr, 0);


	//Get all jobs running on that node
	jobs_running_in_node = malloc (sizeof(slurm_job_info_t) * job_ptr->record_count);

	cont = 0;
	for (i = 0; i < job_ptr->record_count; i++){
		job_info = job_ptr->job_array[i];
//		printf ("Job %d is running on %s\n",job_ptr->job_array[i].job_id, job_ptr->job_array[i].nodes );
		hl = hostlist_create(job_info.nodes);
		if (hostlist_find(hl, drain_node) == -1)
			continue;

		if (_migrate_job(job_info.job_id, NO_VAL, destination_nodes, partition, drain_node, shared, spread, true) !=0){
			printf ("Job %d cannot be migrated, aborting.\n",job_info.job_id);
			return SLURM_ERROR;
			}
		printf ("We need to migrate job %d\n ",job_info.job_id);
		jobs_running_in_node[cont] = job_info;
		cont +=1;
	}


	printf("Verification completed. Starting migration\n");

	for (i = 0; i < cont; i++){
		job_info = jobs_running_in_node[i];
		printf ("Migrating job %d\n ", job_info.job_id);

		 //Migration is a slow process, so we check task status just before migrating to avoid race conditions
		 // (it may have finished while migrating the preivous one)
		if ((error = slurm_load_job (&job_ptr, job_info.job_id, 0)) != SLURM_SUCCESS )
			continue;

		job_info = job_ptr->job_array[job_ptr->record_count-1];
		if (job_info.job_state  != JOB_RUNNING) {
			continue;

		//after all verifications, migrate
		if (_migrate_job(job_info.job_id, NO_VAL, destination_nodes, partition, drain_node, shared, spread, false) !=0){
		        printf ("Job %d could not be migrated, aborting node draining. Cancel it manually and try again.\n", job_info.job_id);
						node_msg.node_state = old_node_state;
						if (slurm_update_node(&node_msg)) {
						 printf ("Could not set node %s into DRAIN status\n",drain_node );
						 return (EMIGRATION_ERROR);
						}
						}


		        return SLURM_ERROR;
	  }
	}

	printf("All jobs migrated, exiting\n");
	slurm_free_job_info_msg(job_ptr);



	return (EMIGRATION_SUCCESS);

}


char** _str_split(char* a_str, const char a_delim)
{
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;

    result = malloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = strdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}

static int _print_existing_jobs (void){

	job_info_msg_t * job_ptr;
	uint16_t show_flags = 0;
	time_t update_time = 0;
	int counter = 0;
	FILE* fout = stdout;
	printf ("PRINT EXSITING JOBS\n");
	printf (	"loading jobs\n");
	slurm_load_jobs (update_time, &job_ptr, show_flags);

	printf ("	jobs loaded\n");
	printf ("	number of elements: %d \n", job_ptr->record_count);

	while (counter < job_ptr->record_count){
		printf ("	printing job %d\n", counter);

		slurm_print_job_info (fout, &job_ptr->job_array[counter], 0);
		counter +=1;
		}
	slurm_free_job_info_msg(job_ptr);

	return 0;

}


static int _job_info_to_job_desc (job_info_t job_info, job_desc_msg_t *job_desc_msg){
    job_desc_msg->account= job_info.account ;
	  job_desc_msg->alloc_node= job_info.alloc_node ;
	  job_desc_msg->comment= job_info.comment ;
	  job_desc_msg->contiguous= job_info.contiguous ;
	  job_desc_msg->dependency= job_info.dependency ;
	  job_desc_msg->end_time= job_info.end_time ;
	  job_desc_msg->exc_nodes= job_info.exc_nodes ;
	  job_desc_msg->features= job_info.features ;
	  job_desc_msg->gres= job_info.gres ;
	  job_desc_msg->group_id= job_info.group_id ;
	  job_desc_msg->licenses= job_info.licenses ;
	  job_desc_msg->name= job_info.name ;
	  job_desc_msg->network= job_info.network ;
	  job_desc_msg->nice= job_info.nice ;
	  job_desc_msg->num_tasks= job_info.num_tasks ;
	  job_desc_msg->partition= job_info.partition ;
	  job_desc_msg->priority= job_info.priority ;
	  job_desc_msg->profile= job_info.profile ;
	  job_desc_msg->qos= job_info.qos ;
	  job_desc_msg->reboot= job_info.reboot ;
	  job_desc_msg->req_nodes= job_info.req_nodes ;
	  job_desc_msg->time_limit= job_info.time_limit ;
	  job_desc_msg->time_min = job_info.time_min;
	  job_desc_msg->user_id = job_info.user_id;
	  job_desc_msg->select_jobinfo= job_info.select_jobinfo ;
	  job_desc_msg->wait4switch= job_info.wait4switch ;
	  job_desc_msg->wckey= job_info.wckey ;
		return 0;
	}
