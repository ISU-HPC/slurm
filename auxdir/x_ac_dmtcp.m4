##*****************************************************************************
## $Id: x_ac_dmtcp.m4 0001 2009-01-10 16:06:05Z hjcao $
##*****************************************************************************
#  AUTHOR:
#    Copied from x_ac_blcr, which is copied from x_ac_munge.
#
#  SYNOPSIS:
#    X_AC_DMTCP()
#
#  DESCRIPTION:
#    Check the usual suspects for an DMTCP installation,
#    updating CPPFLAGS and LDFLAGS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_DMTCP], [

  _x_ac_dmtcp_dirs="/usr /usr/local /opt/freeware /opt/dmtcp"
  _x_ac_dmtcp_libs="dmtcp.h"

  AC_ARG_WITH(
    [dmtcp],
    AS_HELP_STRING(--with-dmtcp=PATH,Specify path to DMTCP installation),
    [_x_ac_dmtcp_dirs="$withval $_x_ac_dmtcp_dirs"])

  AC_CACHE_CHECK(
    [for dmtcp installation],
    [x_ac_cv_dmtcp_dir],
    [
      for d in $_x_ac_dmtcp_dirs; do
	test -d "$d" || continue
	test -d "$d/include" || continue
#	test -f "$d/include/dmtcp.h" || continue
	for lib in $_x_ac_dmtcp_libs; do
	  test -f "$d/include/$lib" || continue
      x_ac_cv_dmtcp_dir=$d
#	  AC_LINK_IFELSE(
#	    [AC_LANG_CALL([], cr_get_restart_info)],
#	    AS_VAR_SET(x_ac_cv_dmtcp_dir, $d))
	done
	test -n "$x_ac_cv_dmtcp_dir" && break
      done
    ])

  if test -z "$x_ac_cv_dmtcp_dir"; then
    AC_MSG_WARN([unable to locate dmtcp installation])
  else
    DMTCP_HOME="$x_ac_cv_dmtcp_dir"
    DMTCP_LIBS=" "
    DMTCP_CPPFLAGS=" "
    DMTCP_LDFLAGS=" "
  fi

  AC_DEFINE_UNQUOTED(DMTCP_HOME, "$x_ac_cv_dmtcp_dir", [Define DMTCP installation home])
  AC_SUBST(DMTCP_HOME)

  AC_SUBST(DMTCP_LIBS)
  AC_SUBST(DMTCP_CPPFLAGS)
  AC_SUBST(DMTCP_LDFLAGS)

  AM_CONDITIONAL(WITH_DMTCP, test -n "$x_ac_cv_dmtcp_dir")
])
