AC_INIT([mimir], [0.9], [taogao@udel.edu])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])
AC_CONFIG_MACRO_DIR([m4])
AM_PROG_AR
AC_PROG_CXX(mpicxx)
AC_CONFIG_HEADERS([src/ac_config.h])
AC_CONFIG_FILES([Makefile src/Makefile examples/Makefile generator/Makefile])
AC_CHECK_LIB(memkind, hbw_posix_memalign, [], [], [])
AX_CXX_COMPILE_STDCXX_11
AC_PROG_RANLIB
AC_OUTPUT
