# Makefile for migration scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

libsched_migration_LDADD = $(top_builddir)/src/plugins/slurmctld/job_migration/libjob_migration.la $(DL_LIBS)

pkglib_LTLIBRARIES = libsched_migration.la

libsched_migration_la_SOURCES = migration_wrapper.c	\
			migration.c	\
			migration.h

libsched_migration_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)

force:
$(libsched_migration_LDADD) : force
	 @cd `dirname $@` && $(MAKE) `basename $@`
