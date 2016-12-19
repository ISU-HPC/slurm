
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
#include <assert.h>

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
<<<<<<< HEAD
static char *optargs;
=======
>>>>>>> migration
static const char cr_checkpoint_start[] = PKGLIBEXECDIR "/cr_start.sh";

static int _enable_multicheckpoint (int val, const char *optarg, int remote);
char** _str_split(char* a_str, const char a_delim);
int _export_env_vars(spank_t sp,char *optarg);


/*
 *  Provide a --with-multicheckpoint option to srun:
 */
struct spank_option spank_options[] =
{
  /*
  //no input params
    { "with-multicheckpoint",NULL, "Allows MULTICHECKPOINT checkpoints on the job being run", 0, 0,
        (spank_opt_cb_f) _enable_multicheckpoint
    },

    */
    { "with-multicheckpoint","Env. vars for checkpoint lib, separated by commas", "Allows MULTICHECKPOINT checkpoints on the job being run", 2, 0,
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

    //EXPORT DECLARED ENVIRONMENT VARS
  if (_export_env_vars(sp, optargs) != 0) {
    slurm_error("error exporting env vars");
    return (ESPANK_BAD_ARG);

  }



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



<<<<<<< HEAD

static int _enable_multicheckpoint (int val, const char *optarg, int remote)
{
    multicheckpoint_enabled=0;
    optargs=strdup(optarg);
    return (ESPANK_SUCCESS);
}


int _export_env_vars(spank_t sp,char *optarg){

  char** tokens;
  char** env_var;

  slurm_error ("ENV VARS: %s", optarg);

  tokens = _str_split(optarg, ',');

  if (tokens)
  {
      int i;
      for (i = 0; *(tokens + i); i++)
      {

          env_var = _str_split(*(tokens + i), '=');
          slurm_error("ENV_VAR=[%s]", *(tokens + i));
          spank_setenv (sp, env_var[0], env_var[1],true);
          free (env_var);

      }
      printf("\n");
      free(tokens);
  }

  return (ESPANK_SUCCESS);
}
=======
>>>>>>> migration


char** _str_split(char* a_str, const char a_delim)
{
<<<<<<< HEAD
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
=======
    multicheckpoint_enabled=0;
    return (ESPANK_SUCCESS);
>>>>>>> migration
}
