##*****************************************************************************
#  AUTHOR:
#    Copied from x_ac_munge.
#
#  SYNOPSIS:
#    X_AC_MULTICHECKPOINT()
#
#  DESCRIPTION:
#    Check the usual suspects for an CRIU installation,
#    updating CPPFLAGS and LDFLAGS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************
AC_DEFUN([X_AC_CRIU], [
  _x_ac_criu_dirs="/usr /usr/local /usr/local/sbin"
  _x_ac_criu_exec="criu"

  AC_ARG_WITH(
    [criu],
    AS_HELP_STRING(--with-criu=PATH,Specify path to CRIU installation),
    [AS_IF([test "x$with_criu" != xno],[_x_ac_criu_dirs="$with_criu $_x_ac_criu_dirs"])])

  if [test "x$with_criu" = xno]; then
    AC_MSG_WARN([support for criu disabled])
  else
    AC_CACHE_CHECK(
      [for criu installation],
      [x_ac_cv_criu_path],
      [
        for d in $_x_ac_criu_dirs; do
          test -d "$d" || continue
          test -x "$d/$_x_ac_criu_exec" && x_ac_cv_criu_path="$d"  && break
        done
      ])

    if test -z "$x_ac_cv_criu_path"; then
      AC_MSG_WARN([unable to locate criu installation])
    else
      CRIU_HOME="$x_ac_cv_criu_path"
    fi

    AC_DEFINE_UNQUOTED(CRIU_HOME, "$x_ac_cv_criu_path", [Define CRIU installation home])
    AC_SUBST(CRIU_HOME)
  fi

  AM_CONDITIONAL(WITH_CRIU, test -n "$x_ac_cv_criu_path")
])
