# Makefile for checkpoint/multicheckpoint plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

if WITH_MULTICHECKPOINT

AM_CPPFLAGS = -DPKGLIBEXECDIR=\"$(pkglibexecdir)\" \
	-I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = checkpoint_multicheckpoint.la
checkpoint_multicheckpoint_la_SOURCES = checkpoint_multicheckpoint.c spank_multicheckpoint.c
checkpoint_multicheckpoint_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)

dist_pkglibexec_SCRIPTS = cr_checkpoint.sh cr_restart.sh cr_start.sh cr_common.sh

force:

$(checkpoint_multicheckpoint_LDADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
else
EXTRA_checkpoint_multicheckpoint_la_SOURCES = checkpoint_multicheckpoint.c spank_multicheckpoint.c
EXTRA_DIST = cr_checkpoint.sh cr_restart.sh cr_start.sh
endif
