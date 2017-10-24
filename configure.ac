# initialize
AC_INIT([mimir], [0.9], [taogao@udel.edu])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])

# macro directory
AC_CONFIG_MACRO_DIR([m4])

# configure files
AC_CONFIG_HEADERS([src/ac_config.h])
AC_CONFIG_FILES([Makefile src/Makefile examples/Makefile generator/Makefile])

# check compiler
AM_PROG_AR
AC_PROG_CXX(mpicxx)
AX_CXX_COMPILE_STDCXX_11
AC_PROG_RANLIB

# check memkind library
AC_CHECK_LIB(memkind, hbw_posix_memalign, [], [], [])

# check papi library
AC_ARG_WITH([papi],
            AS_HELP_STRING([--with-papi], [papi directory])],
            [with_papi=yes], [with_papi=no])
AS_IF([test "x$with_papi" != xno],
      [PKG_CHECK_MODULES([PAPI], [papi > 5.5], 
                         [with_papi=yes], [with_papi=no])])
AS_IF([test "x$with_papi" != xno],
      [AC_CHECK_LIB(papi, PAPI_library_init)],[])

AC_OUTPUT
