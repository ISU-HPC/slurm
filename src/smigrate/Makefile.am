#
# Makefile for smigrate

AUTOMAKE_OPTIONS = foreign

AM_CPPFLAGS = -I$(top_srcdir)

bin_PROGRAMS = smigrate

smigrate_LDADD = 	$(top_builddir)/src/api/libslurm.o $(top_builddir)/src/plugins/slurmctld/job_migration/job_migration.o $(DL_LIBS)

noinst_HEADERS = opt.h
smigrate_SOURCES = smigrate.c  opt.c

force:
$(smigrate_LDADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`

smigrate_LDFLAGS = -export-dynamic $(CMD_LDFLAGS)
