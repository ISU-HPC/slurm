# Makefile for migration scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = sched_migration.la

sched_migration_la_SOURCES = migration_wrapper.c	\
			migration.c	\
			migration.h


sched_migration_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
sched_migration_la_LIBADD = ../../slurmctld/job_migration/libjob_migration.la
