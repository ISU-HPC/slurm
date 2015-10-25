
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
#include "src/common/xmalloc.h"

/*
 * All spank plugins must define this macro for the SLURM plugin loader.
 */
SPANK_PLUGIN(dmtcp_spank, 2);

/*
 *  Minimum allowable value for priority. May be set globally
 *   via plugin option min_prio=<prio>
 */

static int dmtcp_enabled=0;
static const uint32_t default_dmtcp_port=7779; // DMTCP default port.
static const uint32_t number_of_coordinators=16; // max number of coordinators running on the same host.
static int _enable_dmtcp (int val, const char *optarg, int remote);


/*
 *  Provide a --with-dmtcp option to srun:
 */
struct spank_option spank_options[] =
{
    { "with-dmtcp",NULL, "Allows DMTCP checkpoints on the job being run", 2, 0,
        (spank_opt_cb_f) _enable_dmtcp
    },
    SPANK_OPTIONS_TABLE_END
};

/*
 *  Called from both srun and slurmd.
 */
int slurm_spank_init (spank_t sp, int ac, char **av)
{
   slurm_error("checkpoint/dmtcp_spank init");
   spank_option_register (sp, spank_options);

    return (0);
}



int slurm_spank_task_init(spank_t sp, int ac, char **av){

  // is DMTCP allowed? If not, exit
  slurm_error("1 ");
  if (dmtcp_enabled != 0)
    return (0);
    slurm_error("2 ");

  //Is this the first job? If so, create env vars and start coordinator

  //job id, needed to access the rest of information
  uint32_t job_id;
  if (spank_get_item (sp, S_JOB_ID, &job_id) != 0){
    slurm_error("Could not get job id");
    return (ESPANK_ERROR);
  }

  char dmtcp_coordinator[1024];
  if (gethostname(dmtcp_coordinator, 1024) != 0){
    slurm_error("Could not get hostname");
    return (ESPANK_ERROR);
  }


  char *ckpt_dir;
  char aux[9];

  if (spank_get_item (sp, S_CHECKPOINT_DIR, &ckpt_dir) != 0){
    slurm_error("Could not get checkpoint dir");
    return (ESPANK_ERROR);
  }

  strcat(ckpt_dir, "/");
  sprintf(aux, "%d", job_id);
  strcat(ckpt_dir, aux);

  char ckpt_file[1024];
  strcpy(ckpt_file, ckpt_dir);
  strcat(ckpt_file, "/dmtcp_coordinator");

  //Create checkpoint dir. If does not exit, it means that I am the first task
  //so this is how I implement concurrence

  if (mkdir(ckpt_dir, S_IRWXU) == 0){
    //get dmtcp po rt from env variable

     char dmtcp_user_port[99];
     int dmtcp_port;
     if ( spank_getenv (sp, "DMTCP_PORT", dmtcp_user_port, 99) == 0)
      dmtcp_port = strtol(dmtcp_user_port, NULL, 10);
    else
      dmtcp_port = default_dmtcp_port;

    char coordinator_exec[1024] = "dmtcp_coordinator --exit-on-last --daemon ";
    sprintf(coordinator_exec, "%s --ckptdir %s", coordinator_exec,ckpt_dir);
    sprintf(coordinator_exec, "%s -p %d", coordinator_exec,dmtcp_port);
    slurm_error("Executing coordinator as %s", coordinator_exec);

    int coordinators=0;
   while (system(coordinator_exec) != 0){
     dmtcp_port +=1;
     coordinators +=1;
     strcpy(coordinator_exec, "dmtcp_coordinator --exit-on-last --daemon ");
     sprintf(coordinator_exec, "%s --ckptdir %s", coordinator_exec,ckpt_dir);
     sprintf(coordinator_exec, "%s -p %d", coordinator_exec,dmtcp_port);
     if (coordinators > number_of_coordinators)
      break;
   }
      //if checkpoint could not be started, we are continuing anyway


      FILE *fp;
      fp=fopen(ckpt_file, "w");
      fprintf(fp, "DMTCP_COORDINATOR=%s\n", dmtcp_coordinator);
      fprintf(fp, "DMTCP_PORT=%d\n", dmtcp_port);
      fclose(fp);


    //we modify the application to be executed by including a DMTCP wrapper.


    char **argv;
    char **newArgv;
    uint32_t argc = 0;
    uint32_t cont = 0;

    spank_get_item (sp, S_JOB_ARGV, &argc,&argv);

    argc += 1;
    newArgv = malloc (sizeof(char*) * (argc + 1));
    newArgv[0] = strdup("/dmtcp_stuff/bin/dmtcp_launch");

    for (cont = 0; cont < argc-1; cont++)
      newArgv[cont+1] = strdup(argv[cont]);
    //newArgv[argc] = NULL;

    if (spank_set_item(sp, S_JOB_ARGV, &argc,&newArgv) != ESPANK_SUCCESS)
      slurm_error("modification did not succeed");
    else
      slurm_error("DMTCP wrapper enabled");
  }

  return (0);

}

int slurm_spank_task_exit(spank_t sp, int ac, char **av){

  //here we want to delete dmtcp_coordinator file and shutdown coordinator
  slurm_error("starting slurm_spank_task_exit :(");

  //job id, needed to access the rest of information
  uint32_t job_id;
  if (spank_get_item (sp, S_JOB_ID, &job_id) != 0){
    slurm_error("Could not get job id");
    return (0);
  }

  char *ckpt_dir;
  char aux[9];
  if (spank_get_item (sp, S_CHECKPOINT_DIR, &ckpt_dir) != 0){
    slurm_error("Could not get checkpoint dir");
    return (0);
  }

  char ckpt_file[1024];
  strcpy(ckpt_file,ckpt_dir);
  strcat(ckpt_file, "/");
  sprintf(aux, "%d", job_id);
  strcat(ckpt_file, aux);

  strcat(ckpt_file, "/dmtcp_coordinator");


  remove (ckpt_file);

  return (0);
 }

static int _enable_dmtcp (int val, const char *optarg, int remote)
{
  slurm_error("spank _enable_dmtcp");

    dmtcp_enabled=0;
    return (0);
}
