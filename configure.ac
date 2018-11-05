AC_PREREQ(2.63)
#
# (c) 2017 by University of Delaware, Argonne National Laboratory, San Diego
#     Supercomputer Center, National University of Defense Technology,
#     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
#
#     See COPYRIGHT in top-level directory.
#

# initialize
# FIX version number and email
m4_include([maint/version.m4])
dnl 2nd arg is itentionally underquoted
AC_INIT([mimir],
        MIMIR_VERSION_m4,
        [taufer@utk.edu])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])

MIMIR_VERSION=MIMIR_VERSION_m4
AC_SUBST([MIMIR_VERSION])
MIMIR_RELEASE_DATE="MIMIR_RELEASE_DATE_m4"
AC_SUBST([MIMIR_RELEASE_DATE])
libmimir_so_version="libmpi_so_version_m4"
AC_SUBST([libmimir_so_version])

# MIMIR_NUMVERSION=`expr $V1$V2$V3$V4$V5 + 0`
AC_SUBST(MIMIR_NUMVERSION)

AC_ARG_WITH(custom-version-string,
            AC_HELP_STRING([--with-custom-version-string], [Adds a user-specified value to the output of the mpichversion executable]),,with_custom_version_string="")
MIMIR_CUSTOM_STRING=$with_custom_version_string
AC_SUBST(MIMIR_CUSTOM_STRING)

# Print out the configure options
CONFIGURE_ARGUMENTS="$ac_configure_args"
AC_SUBST(CONFIGURE_ARGUMENTS)
if test -n "$ac_configure_args" ; then
    echo "Configuring Mimir version $MIMIR_VERSION with $ac_configure_args"
else 
    echo "Configuring Mimir version $MIMIR_VERSION"
fi

# Add the information on the system:
echo "Running on system: `uname -a`"

# macro directory
AC_CONFIG_MACRO_DIR([m4])

# configure files
AC_CONFIG_HEADERS([src/mimir_config.h])
AH_TOP([//
// (c) 2016 by University of Delaware, Argonne National Laboratory, San Diego
//     Supercomputer Center, National University of Defense Technology,
//     National Supercomputer Center in Guangzhou, and Sun Yat-sen University.
//
//     See COPYRIGHT in top-level directory.
//

#ifndef MIMIR_CONFIG_H_INCLUDED
#define MIMIR_CONFIG_H_INCLUDED
])
AH_BOTTOM([
#endif // MIMIR_CONFIG_H_INCLUDED
])

AC_CONFIG_FILES([Makefile src/Makefile examples/Makefile generator/Makefile])

# check compiler
AM_PROG_AR
AC_PROG_CXX([mpicxx])
AX_CXX_COMPILE_STDCXX_11
AC_PROG_RANLIB

AC_CHECK_HEADERS([malloc.h])

# check for MPI library
AC_CHECK_HEADERS([mpi.h], [], [AC_MSG_ERROR([Cannot found MPI library])])
AC_CHECK_LIB(mpi, MPI_Init, [], [AC_MSG_ERROR([Compiler cannot link MPI library])], [])

# check memkind library
AC_CHECK_HEADERS([memkind.h])
AC_CHECK_LIB(memkind, hbw_posix_memalign, [AC_DEFINE([HAVE_LIBMEMKIND], [1], [Define to enable memkind])], [], [])

# check papi library
AC_ARG_WITH([papi],
            AS_HELP_STRING([--with-papi], [papi directory]),
            [with_papi=yes], [with_papi=no])
AS_IF([test "x$with_papi" = "xyes"],
      [PKG_CHECK_MODULES([PAPI], [papi > 1.0], 
                         [with_papi=yes], [with_papi=no])],
                         [with_papi=no])
AS_IF([test "x$with_papi" = "xyes"],
      [AC_CHECK_LIB(papi, PAPI_library_init, [], [AC_MSG_ERROR([Compiler cannot link PAPI library])])],[])

if test "x$with_papi" = "xyes"; then
    AC_DEFINE([HAVE_PAPI], [1], [define to enable PAPI support])
fi

AC_OUTPUT
