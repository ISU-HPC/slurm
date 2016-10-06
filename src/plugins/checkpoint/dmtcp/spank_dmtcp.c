
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

#define MAX_PATH_LEN 1024
#define BUFSIZE 128


/*
 * All spank plugins must define this macro for the SLURM plugin loader.
 */
SPANK_PLUGIN(dmtcp_spank, 2);

/*
 *  Minimum allowable value for priority. May be set globally
 *   via plugin option min_prio=<prio>
 */

static int dmtcp_enabled=1;
static const uint32_t default_dmtcp_port=7779; // DMTCP default port.
static const char cr_checkpoint_start[] = PKGLIBEXECDIR "/cr_start.sh";

static int _enable_dmtcp (int val, const char *optarg, int remote);



//employed to find a command in remote hosts, as user profile is not loaded with SPANK
int find_command(char* command, char* full_command) {
    char cmd[MAX_PATH_LEN];
    char c;
    FILE *fp;

    sprintf(cmd, "source /etc/profile && whereis %s", command);

    if ((fp = popen(cmd, "r")) == NULL) {
        printf("Error opening pipe!\n");
        return -1;
    }

    int initFound=0;
    full_command[0]=0;

    while (1) {
        c = fgetc(fp);
        if((c!= EOF) && (c!='\n') ){
          if (!initFound){
            if (c != '/')
              continue;
            initFound=1;
          }

        if (initFound && (c==' ')) //in case the command is present on more than 1 place, we take the first
          break;

        sprintf(full_command, "%s%c", full_command, c);
        }

        else
            break;                  // exit when you hit the end of the file.
    }

    if(pclose(fp))  {
        printf("Command not found or exited with error status\n");
        return -1;
    }
    return 0;
}



/*
 *  Provide a --with-dmtcp option to srun:
 */
struct spank_option spank_options[] =
{
    { "with-dmtcp",NULL, "Allows DMTCP checkpoints on the job being run", 0, 0,
        (spank_opt_cb_f) _enable_dmtcp
    },
    SPANK_OPTIONS_TABLE_END
};

/*
 *  Called from both srun and slurmd.
 */
int slurm_spank_init (spank_t sp, int ac, char **av)
{
   //info("checkpoint/dmtcp_spank init");
   spank_option_register (sp, spank_options);
   return (ESPANK_SUCCESS);
}


 int slurm_spank_task_init(spank_t sp, int ac, char **av){
   if (dmtcp_enabled != 0)
    return (ESPANK_SUCCESS);

    //we modify the application to be executed by including a DMTCP wrapper.
  char **argv;
  char **newArgv;
  uint32_t argc = 0;
  uint32_t cont = 0;

  spank_get_item (sp, S_JOB_ARGV, &argc,&argv);

  argc += 1;
  newArgv = xmalloc (sizeof(char*) * (argc + 1));
  newArgv[0] = strdup(cr_checkpoint_start);


  //slurm_error("printing newArgv");
  for (cont = 0; cont < argc-1; cont++) {
    newArgv[cont+1] = strdup(argv[cont]);
  //  slurm_error(newArgv[cont+1]);
  }
  //newArgv[argc] = NULL;

  if (spank_set_item(sp, S_JOB_ARGV, &argc,&newArgv) != ESPANK_SUCCESS) {
    slurm_error("DMTCP Plugin could not be enabled");
    return (ESPANK_ERROR);

  }

return (ESPANK_SUCCESS);
}


// //TODO when everything is tested, add --quiet to dmtcp_coordinator calls

// int slurm_spank_task_init(spank_t sp, int ac, char **av){
//
//   if (dmtcp_enabled != 0)
//     return (ESPANK_SUCCESS);
//
//   //Is this the first job? If so, create env vars and start coordinator
//
//   //job id, needed to access the rest of information
//   uint32_t job_id;
//   if (spank_get_item (sp, S_JOB_ID, &job_id) != 0){
//     slurm_error("Could not get job id");
//     return (ESPANK_ERROR);
//   }
//
//   char dmtcp_coordinator[MAX_PATH_LEN];
//   if (gethostname(dmtcp_coordinator, MAX_PATH_LEN) != 0){
//     slurm_error("Could not get hostname");
//     return (ESPANK_ERROR);
//   }
//
//   char *ckpt_dir;
//   char ckpt_file[MAX_PATH_LEN];
//
//   if (spank_get_item (sp, S_CHECKPOINT_DIR, &ckpt_dir) != 0){
//     slurm_error("Could not get checkpoint dir");
//     return (ESPANK_ERROR);
//   }
//
//   // max number of coordinators running on a host is, max case, number of serial tasks that can run on that node
//   int number_of_coordinators=16;
//   time_t update_time = 0;
//   node_info_msg_t *resp;
//   uint16_t show_flags = 0;
//
//   if (!slurm_load_node(update_time, &resp, show_flags))
//     number_of_coordinators = resp->node_array[0].cores * resp->node_array[0].sockets; //TODO this may be a problem on heterogeneous clusters
//
//   slurm_free_node_info_msg(resp);
//
//
//   sprintf(ckpt_dir,"%s/%d", ckpt_dir, job_id);
//   sprintf(ckpt_file,"%s/dmtcp_coordinator", ckpt_dir);
//
//   char full_dmtcp_coordinator_command[MAX_PATH_LEN];
//   char* command="dmtcp_coordinator";
//   find_command(command, full_dmtcp_coordinator_command);
//
//   //Create checkpoint dir. If does not exit, it means that I am the first task
//   //so this is how I implement concurrence
//   if (mkdir(ckpt_dir, S_IRWXU) == 0){
//     //get dmtcp po rt from env variable
//
//      char dmtcp_user_port[99];
//      int dmtcp_port;
//      if ( spank_getenv (sp, "DMTCP_PORT", dmtcp_user_port, 99) == 0)
//       dmtcp_port = strtol(dmtcp_user_port, NULL, 10);
//     else
//       dmtcp_port = default_dmtcp_port;
//
//    char coordinator_exec[MAX_PATH_LEN];
//    sprintf(coordinator_exec, "%s --exit-on-last --daemon --ckptdir %s -p %d", full_dmtcp_coordinator_command,
//             ckpt_dir,dmtcp_port);
//    //slurm_error("Executing coordinator as %s", coordinator_exec);
//
//     int coordinators=0;
//    while (system(coordinator_exec) != 0){
//      dmtcp_port +=1;
//      coordinators +=1;
//      sprintf(coordinator_exec, "%s --exit-on-last --daemon --ckptdir %s -p %d",full_dmtcp_coordinator_command,
//               ckpt_dir,dmtcp_port);
//      if (coordinators > number_of_coordinators){
//        slurm_error("Could not start coordinator");
//        return (ESPANK_ERROR);
//      }
//    }
//       //if checkpoint could not be started, we are continuing anyway
//
//       FILE *fp;
//       fp=fopen(ckpt_file, "w");
//       fprintf(fp, "DMTCP_COORD_HOST=%s\n", dmtcp_coordinator);
//       fprintf(fp, "DMTCP_COORD_PORT=%d\n", dmtcp_port);
//       fclose(fp);
//
//
//     //we modify the application to be executed by including a DMTCP wrapper.
//     char **argv;
//     char **newArgv;
//     uint32_t argc = 0;
//     uint32_t cont = 0;
//
//     spank_get_item (sp, S_JOB_ARGV, &argc,&argv);
//
//
//     char full_dmtcp_launch_command[MAX_PATH_LEN];
//     command="dmtcp_launch";
//
//     find_command(command, full_dmtcp_launch_command);
//
//
//     argc += 5;
//     newArgv = xmalloc (sizeof(char*) * (argc + 1));
//     newArgv[0] = strdup(full_dmtcp_launch_command);
//     newArgv[1] = strdup("--ckptdir");
//     newArgv[2] = strdup(ckpt_dir);
//     newArgv[3] = strdup("-p");
//     sprintf(dmtcp_user_port, "%d", dmtcp_port);
//     newArgv[4] = strdup(dmtcp_user_port);
//
//
//     for (cont = 0; cont < argc-5; cont++)
//       newArgv[cont+5] = strdup(argv[cont]);
//     //newArgv[argc] = NULL;
//
//     if (spank_set_item(sp, S_JOB_ARGV, &argc,&newArgv) != ESPANK_SUCCESS) {
//       slurm_error("DMTCP Plugin could not be enabled");
//       return (ESPANK_ERROR);
//
//     }
//   }
//
//   return (ESPANK_SUCCESS);
//
// }

int slurm_spank_task_exit(spank_t sp, int ac, char **av){

  //here we want to delete dmtcp_coordinator file and shutdown coordinator
  //slurm_error("starting slurm_spank_task_exit :(");

  //job id, needed to access the rest of information
  uint32_t job_id;
  if (spank_get_item (sp, S_JOB_ID, &job_id) != 0){
    slurm_error("Could not get job id");
    return (ESPANK_ERROR);
  }

  char *ckpt_dir;
  if (spank_get_item (sp, S_CHECKPOINT_DIR, &ckpt_dir) != 0){
    slurm_error("Could not get checkpoint dir");
    return (ESPANK_ERROR);
  }

  char ckpt_file[MAX_PATH_LEN];
  sprintf(ckpt_file,"%s/%d/dmtcp_coordinator", ckpt_dir, job_id);
  remove (ckpt_file);

  return (ESPANK_SUCCESS);
 }

static int _enable_dmtcp (int val, const char *optarg, int remote)
{
    dmtcp_enabled=0;
    return (0);
}
