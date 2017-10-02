##*****************************************************************************
#  AUTHOR:
#    Copied from x_ac_munge.
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
  _x_ac_dmtcp_dirs="/usr /usr/local /usr/local/sbin"
  _x_ac_dmtcp_exec="dmtcp_launch"

  AC_ARG_WITH(
    [dmtcp],
    AS_HELP_STRING(--with-dmtcp=PATH,Specify path to DMTCP installation),
    [AS_IF([test "x$with_dmtcp" != xno],[_x_ac_dmtcp_dirs="$with_dmtcp $_x_ac_dmtcp_dirs"])])

  if [test "x$with_dmtcp" = xno]; then
    AC_MSG_WARN([support for dmtcp disabled])
  else
    AC_CACHE_CHECK(
      [for dmtcp installation],
      [x_ac_cv_dmtcp_path],
      [
        for d in $_x_ac_dmtcp_dirs; do
          test -d "$d" || continue
          test -x "$d/bin/$_x_ac_dmtcp_exec" && x_ac_cv_dmtcp_path="$d"  && break
        done
      ])

    if test -z "$x_ac_cv_dmtcp_path/bin"; then
      AC_MSG_WARN([unable to locate dmtcp installation])
    else
      DMTCP_HOME="$x_ac_cv_dmtcp_path/bin"
    fi

    AC_DEFINE_UNQUOTED(DMTCP_HOME, "$x_ac_cv_dmtcp_path", [Define DMTCP installation home])
    AC_SUBST(DMTCP_HOME)
  fi

  AM_CONDITIONAL(WITH_DMTCP, test -n "$x_ac_cv_dmtcp_path")
])
