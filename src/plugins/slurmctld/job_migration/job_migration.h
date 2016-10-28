#ifndef __MIGRATION_API_H__
#define __MIGRATION_API_H__


/* BEGIN_C_DECLS should be used at the beginning of your declarations,
   so that C++ compilers don't mangle their names.  Use END_C_DECLS at
   the end of C declarations. */
#undef BEGIN_C_DECLS
#undef END_C_DECLS
#ifdef __cplusplus
# define BEGIN_C_DECLS	extern "C" {
# define END_C_DECLS	}
#else
# define BEGIN_C_DECLS	/* empty */
# define END_C_DECLS	/* empty */
#endif

/* PARAMS is a macro used to wrap function prototypes, so that compilers
   that don't understand ANSI C prototypes still work, and ANSI C
   compilers can issue warnings about type mismatches.  */
#undef PARAMS
#if defined (__STDC__) || defined (_AIX)			\
	|| (defined (__mips) && defined (_SYSTYPE_SVR4))	\
	|| defined(WIN32) || defined(__cplusplus)
# define PARAMS(protos)	protos
#else
# define PARAMS(protos)	()
#endif

BEGIN_C_DECLS

enum migration_err {
    EMIGRATION_SUCCESS     = 0, /* Success.                                   */
    EMIGRATION_ERROR       = 1, /* Generic error.                             */
    EMIGRATION_BAD_ARG     = 2, /* Bad argument.                              */
    EMIGRATION_NOT_JOB	   = 3, /* Job does not exist.                        */
		EMIGRATION_JOB_ERROR	 = 4, /* Error on job																*/
    EMIGRATION_DEST_ERROR  = 5, /* Error on destination nodes                 */
};

typedef enum migration_err migration_err_t;

/* migrates a job after it has been checkpointed. */
extern int slurm_checkpoint_migrate (uint32_t job_id, uint32_t step_id,char *destination_nodes);

END_C_DECLS
#endif
