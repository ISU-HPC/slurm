# Makefile for migration scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

sched_migration_compact_la_LIBADD = $(top_builddir)/src/plugins/slurmctld/job_migration/libjob_migration.la $(DL_LIBS)

pkglib_LTLIBRARIES = sched_migration_compact.la

sched_migration_compact_la_SOURCES = migration_wrapper.c	\
			migration.c	\
			migration.h

sched_migration_compact_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)

force:
$(sched_migration_compact_LDADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
