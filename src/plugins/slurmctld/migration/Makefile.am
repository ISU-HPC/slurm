# Makefile for backfill scheduler plugin

AUTOMAKE_OPTIONS = foreign

AM_CXXFLAGS = -fexceptions

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = slurmctld_migration.la

slurmctld_migration_la_SOURCES = migration.c migration.h
slurmctld_migration_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)
