# Makefile for checkpoint/criu plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

if WITH_CRIU

AM_CPPFLAGS = -DPKGLIBEXECDIR=\"$(pkglibexecdir)\" \
	-I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = checkpoint_criu.la

checkpoint_criu_la_SOURCES = checkpoint_criu.c
checkpoint_criu_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)

dist_pkglibexec_SCRIPTS = cr_checkpoint.sh cr_restart.sh

force:

$(checkpoint_criu_LDADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
else

#EXTRA_checkpoint_criu_la_SOURCES = checkpoint_criu.c spank_criu.c
EXTRA_checkpoint_criu_la_SOURCES = checkpoint_criu.c

EXTRA_DIST = cr_checkpoint.sh cr_restart.sh
endif
