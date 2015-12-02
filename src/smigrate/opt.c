/*****************************************************************************\
 *  opt.c - options processing for smigrate
 *****************************************************************************
 *  Copyright (C) 2002-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Mark Grondona <grondona1@llnl.gov>, et. al.
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/

#if HAVE_CONFIG_H
#  include "config.h"
#endif

#include <string.h>		/* strcpy, strncasecmp */

#ifdef HAVE_STRINGS_H
#  include <strings.h>
#endif

#ifndef _GNU_SOURCE
#  define _GNU_SOURCE
#endif

#if HAVE_GETOPT_H
#  include <getopt.h>
#else
#  include "src/common/getopt.h"
#endif

#ifdef HAVE_LIMITS_H
#  include <limits.h>
#endif

#include <fcntl.h>
#include <stdarg.h>		/* va_start   */
#include <stdio.h>
#include <stdlib.h>		/* getenv     */
#include <ctype.h>		/* isdigit    */
#include <sys/param.h>		/* MAXPATHLEN */
#include <sys/stat.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/utsname.h>

#include "src/common/list.h"
#include "src/common/log.h"
#include "src/common/parse_time.h"
#include "src/common/plugstack.h"
#include "src/common/proc_args.h"
#include "src/common/read_config.h" /* contains getnodename() */
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_resource_info.h"
#include "src/common/slurm_rlimits_info.h"
#include "src/common/slurm_acct_gather_profile.h"
#include "src/common/uid.h"
#include "src/common/xmalloc.h"
#include "src/common/xstring.h"
#include "src/common/util-net.h"

#include "src/smigrate/opt.h"

//TODO: limpiar esta lista
/* generic OPT_ definitions -- mainly for use with env vars  */
#define OPT_NONE        0x00
#define OPT_INT         0x01
#define OPT_STRING      0x02
#define OPT_DEBUG       0x03
#define OPT_NODES       0x04
#define OPT_BOOL        0x05
#define OPT_CORE        0x06
#define OPT_CONN_TYPE	0x07
#define OPT_DISTRIB	0x08
#define OPT_NO_ROTATE	0x09
#define OPT_GEOMETRY	0x0a
#define OPT_MULTI	0x0b
#define OPT_EXCLUSIVE	0x0c
#define OPT_OVERCOMMIT	0x0d
#define OPT_OPEN_MODE	0x0e
#define OPT_ACCTG_FREQ  0x0f
#define OPT_NO_REQUEUE  0x10
#define OPT_REQUEUE     0x11
#define OPT_MEM_BIND    0x13
#define OPT_WCKEY       0x14
#define OPT_SIGNAL      0x15
#define OPT_GET_USER_ENV  0x16
#define OPT_EXPORT        0x17
#define OPT_CLUSTERS      0x18
#define OPT_TIME_VAL      0x19
#define OPT_CORE_SPEC     0x1a
#define OPT_ARRAY_INX     0x20
#define OPT_PROFILE       0x21
#define OPT_HINT	  0x22

/* generic getopt_long flags, integers and *not* valid characters */


//TODO last three have been redefined to keep the numbering secuential It might be a problem
//if declared with a different value somewhere else... we'll see

#define LONG_OPT_JOBID       0x105
#define LONG_OPT_TEST_ONLY       0x106
#define LONG_OPT_PRIORITY        0x107
#define LONG_OPT_STEPID	0x108
/*---- global variables, defined in opt.h ----*/
opt_t opt;
int error_exit = 1;

/*---- forward declarations of static functions  ----*/

typedef struct env_vars env_vars_t;

static void  _help(void);

/* fill in default options  */
static void _opt_default(void);

static void _set_options(int argc, char **argv);

static bool _opt_verify(void);

static int _get_int(const char *arg, const char *what);

static void _opt_list(void);

static void _usage(void);

/*---[ end forward declarations of static functions ]---------------------*/

/*
 * print error message to stderr with opt.progname prepended
 */
#undef USE_ARGERROR
#if USE_ARGERROR
static void argerror(const char *msg, ...)
  __attribute__ ((format (printf, 1, 2)));
static void argerror(const char *msg, ...)
{
	va_list ap;
	char buf[256];

	va_start(ap, msg);
	vsnprintf(buf, sizeof(buf), msg, ap);

	fprintf(stderr, "%s: %s\n",
		opt.progname ? opt.progname : "smigrate", buf);
	va_end(ap);
}
#else
#  define argerror error
#endif				/* USE_ARGERROR */


/*
 * _opt_default(): used by initialize_and_process_args to set defaults
 */
static void _opt_default()
{
	opt.jobid    = NO_VAL;
	opt.stepid    = 0;
	opt.nodes = "";
	opt.hold	    = false;
	opt.priority = 0;
	opt.test_only   = false;
	opt.quiet = 0;
	opt.verbose = 0;

}




/*---[ command line option processing ]-----------------------------------*/

//TODO anadir NODENAME y los demas que faltan
static struct option long_options[] = {

	{"help",          no_argument,       0, 'h'},
	{"hold",          no_argument,       0, 'H'}, /* undocumented */
	{"jobid",         required_argument, 0, LONG_OPT_JOBID},
	{"nodes",         required_argument, 0, 'N'},
	{"priority",      required_argument, 0, LONG_OPT_PRIORITY},
	{"quiet",         no_argument,       0, 'Q'},
	{"stepid",        required_argument, 0, LONG_OPT_STEPID},
	{"test-only",     no_argument,       0, LONG_OPT_TEST_ONLY},
	{"usage",         no_argument,       0, 'u'},
	{"verbose",       no_argument,       0, 'v'},
	{"version",       no_argument,       0, 'V'},
	{NULL,            0,                 0, 0}
};


//TODO filtrar los que no valgan
static char *opt_string =
	"+ba:A:B:c:C:d:D:e:F:g:hHi:IJ:kL:m:M:n:N:o:Op:P:QRsS:t:uU:vVw:x:";
char *pos_delimit;



//TODO: RETURN VALUES
/*
 * process_options_first_pass()
 *
 * In this first pass we only look at the command line options, and we
 * will only handle a few options (help, usage, quiet, verbose, version),
 * and look for the script name and arguments (if provided).
 *
 * We will parse the environment variable options, batch script options,
 * and all of the rest of the command line options in
 * process_options_second_pass().
 *
 * Return a pointer to the batch script file name is provided on the command
 * line, otherwise return NULL, and the script will need to be read from
 * standard input.
 */
char *process_options_first_pass(int argc, char **argv)
{
	int opt_char, option_index = 0;
	char *str = NULL;

	struct option *optz = spank_option_table_create(long_options);

	if (!optz) {
		error("Unable to create options table");
		exit(error_exit);
	}

	/* initialize option defaults */
	_opt_default();

	optind = 0;

	while ((opt_char = getopt_long(argc, argv, opt_string,
				       optz, &option_index)) != -1) {
		switch (opt_char) {
		case '?':
			fprintf(stderr, "Try \"smigrate --help\" for more "
				"information\n");
			exit(error_exit);
			break;
		case 'h':
			_help();
			exit(0);
			break;
		case 'Q':
			opt.quiet++;
			break;
		case 'u':
			_usage();
			exit(0);
		case 'v':
			opt.verbose++;
			break;
		case 'V':
			print_slurm_version();
			exit(0);
			break;
		default:
			/* will be parsed in second pass function */
			break;
		}
	}
	xfree(str);
	spank_option_table_destroy(optz);

	return NULL;
}

/* process options:
 * 1. update options with option set in the script
 * 2. update options with env vars
 * 3. update options with commandline args
 * 4. perform some verification that options are reasonable
 */
int process_options_second_pass(int argc, char *argv[], const char *file,
				const void *script_body, int script_size)
{
	/* set options from command line */
	_set_options(argc, argv);

	if (!_opt_verify())
		exit(error_exit);

	if (opt.verbose > 3)
		_opt_list();

	return 1;

}


//TODO ver los que hay, anadir NODENAME y los que faltan

static void _set_options(int argc, char **argv)
{
	int opt_char, option_index = 0, max_val = 0;
	char *tmp;

	struct option *optz = spank_option_table_create(long_options);

	if (!optz) {
		error("Unable to create options table");
		exit(error_exit);
	}

	optind = 0;
	while ((opt_char = getopt_long(argc, argv, opt_string,
			optz, &option_index)) != -1) {
		switch (opt_char) {
		case '?':
			error("Try \"smigrate --help\" for more information");
			exit(error_exit);
			break;
		case 'h':
			_help();
			exit(0);
		case 'H':
			opt.hold = true;
			break;
		case LONG_OPT_JOBID:
			opt.jobid = _get_int(optarg, "jobid");
			break;
		case 'N':
			opt.nodes = optarg;
			break;
		case LONG_OPT_PRIORITY: {
			long long priority = strtoll(optarg, NULL, 10);
			if (priority < 0) {
				error("Priority must be >= 0");
				exit(error_exit);
			}
			opt.priority = priority;
			break;
		}
		case LONG_OPT_STEPID:
			opt.stepid = _get_int(optarg, "stepid");
			break;
		case 'Q':
			opt.quiet++;
			break;
		case LONG_OPT_TEST_ONLY:
			opt.test_only = true;
			break;
		case 'u':
			_usage();
			exit(0);
		case 'v':
			opt.verbose++;
			break;
		case 'V':
			print_slurm_version();
			exit(0);
			break;
		}
	}
	spank_option_table_destroy (optz);
}

/*
 * _opt_verify : perform some post option processing verification
 *
 */
static bool _opt_verify(void)
{
	bool verified = true;
	char *dist = NULL, *lllp_dist = NULL;
	uint32_t cluster_flags = slurmdb_setup_cluster_flags();

	if (opt.quiet && opt.verbose) {
		error ("don't specify both --verbose (-v) and --quiet (-Q)");
		verified = false;
	}

	return verified;
}

/*
 *  Get a decimal integer from arg.
 *
 *  Returns the integer on success, exits program on failure.
 *
 */
static int _get_int(const char *arg, const char *what)
{
	char *p;
	long int result = strtol(arg, &p, 10);

	if ((*p != '\0') || (result < 0L)) {
		error ("Invalid numeric value \"%s\" for %s.", arg, what);
		exit(error_exit);
	}

	if (result > INT_MAX) {
		error ("Numeric argument (%ld) to big for %s.", result, what);
	}

	return (int) result;
}



#define tf_(b) (b == true) ? "true" : "false"


//TODOwhat is this for?

static void _opt_list(void)
{
	char *str;

	info("defined options for program `%i'", opt.jobid);
	info("----------------- ---------------------");

	info("hold           : %s", opt.hold ? "True" : "False" );
	info("jobid             : %u %s", opt.jobid);
	info("nodes           : %s", opt.nodes);
	info("priority           : %u", opt.priority);
	info("quiet           : %s", opt.quiet ? "True" : "False" );
	info("stepid           : %u", opt.stepid);
	info("test_only           : %s", opt.test_only ? "True" : "False" );
	info("verbose           : %s", opt.verbose ? "True" : "False" );

	xfree(str);

}


//TODO esto esta parece incompleto. Xq hay más opciones en _help?
static void _usage(void)
{
 	printf("Usage: smigrate  --jobid=id   --nodes=node1,node2... --verbose  \n");

}


//TODO falta alguna opcion aqui tb
static void _help(void)
{
	slurm_ctl_conf_t *conf;

	printf (
"Usage: smigrate [OPTIONS...] --jobid=id --nodes=node1,node2...\n"
"\n"
"Run options:\n"
"      --jobid=id              id of the job to migrate\n"
"  -Q, --quiet                 quiet mode (suppress informational messages)\n"
"  -v, --verbose               verbose mode (multiple -v's increase verbosity)\n"
"  -H, --hold                  submit job in held state\n"
"      --priority=value        set the priority of the job to value\n"
"      --test-only             check whether the operation can be performed, but not perform it\n"
"      --stepid=id             perform migration on selected step [NOT IMPLEMENTED]\n"
"\n"
"Help options:\n"
"  -h, --help                  show this help message\n"
"  -u, --usage                 display brief usage message\n"
"\n"
"Other options:\n"
"  -V, --version               output version information and exit\n"
"\n"
		);

}
