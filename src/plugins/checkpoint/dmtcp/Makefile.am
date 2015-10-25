# Makefile for checkpoint/dmtcp plugin

AUTOMAKE_OPTIONS = foreign

PLUGIN_FLAGS = -module -avoid-version --export-dynamic

if WITH_DMTCP

AM_CPPFLAGS = -DPKGLIBEXECDIR=\"$(pkglibexecdir)\" \
	-I$(top_srcdir) -I$(top_srcdir)/src/common

pkglib_LTLIBRARIES = checkpoint_dmtcp.la
checkpoint_dmtcp_la_SOURCES = checkpoint_dmtcp.c spank_dmtcp.c
checkpoint_dmtcp_la_LDFLAGS = $(SO_LDFLAGS) $(PLUGIN_FLAGS)

dist_pkglibexec_SCRIPTS = cr_checkpoint.sh cr_restart.sh

force:

$(checkpoint_dmtcp_LDADD) : force
	@cd `dirname $@` && $(MAKE) `basename $@`
else
EXTRA_checkpoint_dmtcp_la_SOURCES = checkpoint_dmtcp.c spank_dmtcp.c
EXTRA_DIST = cr_checkpoint.sh cr_restart.sh
endif
