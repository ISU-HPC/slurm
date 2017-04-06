# Makefile for migration scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions 

AM_CFLAGS = $(top_srcdir)/src/plugins/slurmctld/job_migration/job_migration.la

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

pkglib_LTLIBRARIES = sched_migration.la

sched_migration_la_SOURCES = migration_wrapper.c	\
			migration.c	\
			migration.h
			
sched_migration_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
