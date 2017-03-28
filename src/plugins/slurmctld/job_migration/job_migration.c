
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

char** str_split(char* a_str, const char a_delim);
static int _print_existing_jobs (void);
static int _job_info_to_job_desc (job_info_t job_info, job_desc_msg_t *job_desc_msg);




extern int slurm_checkpoint_migrate (uint32_t job_id, uint32_t step_id, char *destination_nodes, int shared, int spread, bool test_only)
{

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

	slurm_job_info_t job_info = job_ptr->job_array[job_ptr->record_count-1];
	if (job_info.job_state  != JOB_RUNNING) {
		slurm_perror ("Jobs must be RUNNING to be migrated");
		return (EMIGRATION_JOB_ERROR);
	}

	time_t start_time;

	//TODO aqui el segundo parametro, NO_VAl, deberia ser step_id. Por algun motivo casca asi que pongo esto.
	if (( error = slurm_checkpoint_able( job_id, NO_VAL, &start_time)) != SLURM_SUCCESS){
		slurm_perror ("Job is not checkpointable");
		return(EMIGRATION_JOB_ERROR);
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

		if (shared != (uint16_t)NO_VAL)
			job_desc_msg_test.shared = shared;

		if (destination_nodes[0] != '\0' )
			job_desc_msg_test.req_nodes = destination_nodes;

		if (spread)
			job_desc_msg_test.bitflags |= SPREAD_JOB;

		will_run_response_msg_t *will_run_resp = NULL;
		if (slurm_job_will_run2(&job_desc_msg_test, &will_run_resp) != SLURM_SUCCESS) {
			slurm_perror("Error: job will not run");
			return(EMIGRATION_ERROR);
			}
		if (test_only)
			return (EMIGRATION_SUCCESS);

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

	/*Change job atributes while in queue*/

	job_desc_msg_t job_desc_msg;
	slurm_init_job_desc_msg(&job_desc_msg);

	job_desc_msg.job_id = job_info.job_id;
	job_desc_msg.priority = NO_VAL - 1;

	if (destination_nodes[0] != '\0' )
		job_desc_msg.req_nodes = destination_nodes;

	if (shared != (uint16_t)NO_VAL)
		job_desc_msg.shared = shared;

		if (spread)
			job_desc_msg.bitflags |= SPREAD_JOB;

	if (slurm_update_job(&job_desc_msg) != 0) {
		slurm_perror("Error setting migration requirements for job");
		return(EMIGRATION_ERROR);
		}


	return (EMIGRATION_SUCCESS);
}



char** str_split(char* a_str, const char a_delim)
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
