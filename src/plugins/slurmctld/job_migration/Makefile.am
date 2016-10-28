# Makefile for task job_migration library

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -avoid-version --export-dynamic -module

AM_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = job_migration.la
job_migration_la_SOURCES = job_migration.c job_migration.h
job_migration_la_LDFLAGS = $(SO_LDFLAGS)  $(PLUGIN_FLAGS)
