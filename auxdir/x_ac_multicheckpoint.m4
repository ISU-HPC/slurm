##*****************************************************************************
#  AUTHOR:
#    Copied from x_ac_munge.
#
#  SYNOPSIS:
#    X_AC_MULTICHECKPOINT()
#
#  DESCRIPTION:
#    Check the usual suspects for an MULTICHECKPOINT installation,
#    updating CPPFLAGS and LDFLAGS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************
#TODO MANUEL: Este es un hack muy muy feo. Quyeremos que si pone "--with-multicheckpoint" se inicialice la variable
#TODO MANUEL: hacer que si inicializas esta se des-inicialice la de CRIU, BLCR y DMTCP

AC_DEFUN([X_AC_MULTICHECKPOINT], [
#sobra?  _x_ac_criu_dirs="NO"

  AC_ARG_WITH(
    [multicheckpoint],
    AS_HELP_STRING(--with-multicheckpoint, Enable multicheckpoint support),
    [AS_IF([test "x$with_multicheckpoint" != xno],[_x_ac_multicheckpoint_dirs="/bin"])])


    #MANUEL remove all this after debug
  if test -z "$_x_ac_multicheckpoint_dirs"; then
    AC_MSG_WARN([multicheckpoint plugin disabled])
  else
    AC_MSG_WARN([multicheckpoint plugin enabled, I guess...])
  fi


  AM_CONDITIONAL(WITH_MULTICHECKPOINT, test -n "$_x_ac_multicheckpoint_dirs")
])
