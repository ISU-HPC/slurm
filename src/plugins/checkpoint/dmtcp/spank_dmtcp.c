
/*
 *   To compile:
 *    gcc -shared -o renice.so renice.c
 *
 */
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <slurm/spank.h>
#include <slurm/slurm.h>

/*
 * All spank plugins must define this macro for the SLURM plugin loader.
 */
SPANK_PLUGIN(renice, 1);

/*
 *  Minimum allowable value for priority. May be set globally
 *   via plugin option min_prio=<prio>
 */

static int dmtcp_enabled=1;
static int dmtcp_port=7779;
static int _enable_dmtcp (int val, const char *optarg, int remote);

extern char **environ;


/*
 *  Provide a --with-dmtcp option to srun:
 */
struct spank_option spank_options[] =
{
    { "with-dmtcp",NULL, "Allow DMTCP checkpoint on the job being run", 2, 0,
        (spank_opt_cb_f) _enable_dmtcp
    },
    SPANK_OPTIONS_TABLE_END
};

/*
 *  Called from both srun and slurmd.
 */
int slurm_spank_init (spank_t sp, int ac, char **av)
{

   spank_option_register (sp, spank_options);

    return (0);
}



int slurm_spank_task_init(spank_t sp, int ac, char **av){

  // is DMTCP allowed? If not, exit
  if (dmtcp_enabled != 0)
    return (0);


  //Is this the first job? If so, create env vars and start coordinator

  //job id, needed to access the rest of information
  uint32_t job_id;
  if (spank_get_item (sp, S_JOB_ID, &job_id) != 0){
    slurm_error("Could not get job id");
    return (-1);
  }

  // I AM NOT SURE THIS IS USEFUL AT ALL. Ill leave it here just in case
  /*
  job_info_msg_t * job_info_msg;
  if (slurm_load_job(&job_info_msg, job_id,0) != 0){
    slurm_error("Could not get job information");
    return (-1);
  }
  slurm_job_info_t job_info = job_info_msg->job_array[0];
  */

  char ** job_env;
  char *env_var;
  int i = 0;

  if (spank_get_item (sp, S_JOB_ENV, &job_env) != 0){
    slurm_error("Could not get job env");
    return (-1);
  }

  char dmtcp_coordinator[1024];
  if (gethostname(dmtcp_coordinator, 1024) != 0){
    slurm_error("Could not get dmtcp_coordinator");
    return (-1);
  }

  char aux[2048];
  char *param;
  char* value;

  while (i < 100){
    env_var = job_env[i];
    strcpy(aux, env_var);
    param=strtok(aux,"=");
    value=strtok(NULL,"=");

    if (strcmp(param, "SLURM_CHECKPOINT_IMAGE_DIR") == 0)
      break;
    i+=1;

  }

  char ckpt_dir[1024];
  strcpy(ckpt_dir, value);
  strcat(ckpt_dir, "/");
  sprintf(aux, "%d", job_id);
  strcat(ckpt_dir, aux);

  char ckpt_file[1024];
  strcpy(ckpt_file, ckpt_dir);
  strcat(ckpt_file, "/dmtcp_coordinator");

  //Create checkpoint dir. If does not exit, it means that I am the first task
  //so this is how I implement concurrence
  if (mkdir(ckpt_dir, S_IRWXU) == 0){
    FILE *fp;
    fp=fopen(ckpt_file, "w");
    fprintf(fp, "DMTCP_COORDINATOR=%s\n", dmtcp_coordinator);
    fprintf(fp, "DMTCP_PORT=%d\n", dmtcp_port);
    fclose(fp);

    //this launches a coordinator that ends when the job dies, whih is just perfect

    char coordinator_exec[1024] = "dmtcp_coordinator --daemon -p ";
    sprintf(aux, "%d", dmtcp_port);
    strcat(coordinator_exec,aux);

    if (system(coordinator_exec) != 0){
      slurm_error("could not start coordinator");
      return(-1);
    }

    //HERE I SHOULD MODIFY THE CALL TO EXEC THE APPLICATION
  }

  //open the file and read variables.
  //note that file is being written by some other task, hence the wait and the read.
  /*
  FILE *fp = NULL;
  while (fp == NULL){
    fp = fopen(ckpt_file, "r");
    sleep(1);
  }
    //read vars
  fclose(fp);
  */

  //HERE I SHOULD MODIFY THE CALL TO EXEC NOTHING



  slurm_error("End of slurm_spank_task_post_fork");

  return (0);

}



static int _enable_dmtcp (int val, const char *optarg, int remote)
{
    dmtcp_enabled=0;
    return (0);
}
