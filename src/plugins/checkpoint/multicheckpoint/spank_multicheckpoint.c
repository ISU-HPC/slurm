
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
SPANK_PLUGIN(multicheckpoint_spank, 3);

/*
 *  Minimum allowable value for priority. May be set globally
 *   via plugin option min_prio=<prio>
 */

static int multicheckpoint_enabled=1;
static const char cr_checkpoint_start[] = PKGLIBEXECDIR "/cr_start.sh";

static int _enable_multicheckpoint (int val, const char *optarg, int remote);



/*
 *  Provide a --with-multicheckpoint option to srun:
 */
struct spank_option spank_options[] =
{
    { "with-multicheckpoint",NULL, "Allows MULTICHECKPOINT checkpoints on the job being run", 0, 0,
        (spank_opt_cb_f) _enable_multicheckpoint
    },
    SPANK_OPTIONS_TABLE_END
};

/*
 *  Called from both srun and slurmd.
 */
int slurm_spank_init (spank_t sp, int ac, char **av)
{
   spank_option_register (sp, spank_options);
   return (ESPANK_SUCCESS);
}



//this one is called before each mpiexec call, and before starting the script

 int slurm_spank_task_init(spank_t sp, int ac, char **av){
   if (multicheckpoint_enabled != 0)
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




static int _enable_multicheckpoint (int val, const char *optarg, int remote)
{
    multicheckpoint_enabled=0;
    return (ESPANK_SUCCESS);
}
