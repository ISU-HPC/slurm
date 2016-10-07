#
AUTOMAKE_OPTIONS = foreign
CLEANFILES = core.*

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/plugins/slurmctld/job_migration/

bin_PROGRAMS = smigrate

smigrate_SOURCES = smigrate.c opt.c opt.h

convenience_libs = $(top_builddir)/src/api/libslurm.o $(top_builddir)/src/plugins/slurmctld/job_migration/job_migration.o

smigrate_LDADD =   $(convenience_libs) $(DL_LIBS)
smigrate_LDFLAGS = -export-dynamic $(CMD_LDFLAGS)

force:
$(convenience_libs) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
