#! /bin/sh
# (c) 2018 by The University of Tennessee Knoxville, Argonne National
#     Laboratory, San Diego Supercomputer Center, National University of
#     Defense Technology, National Supercomputer Center in Guangzhou,
#     and Sun Yat-sen University.

########################################################################
## Utility functions
########################################################################

recreate_tmp() {
    rm -rf .tmp
    mkdir .tmp 2>&1 >/dev/null
}

warn() {
    echo "===> WARNING: $@"
}

error() {
    echo "===> ERROR:   $@"
}

echo_n() {
    # "echo -n" isn't portable, must portably implement with printf
    printf "%s" "$*"
}

# Assume Program's install-dir is <install-dir>/bin/<prog>.
# Given program name as the 1st argument,
# the install-dir is returned is returned in 2nd argument.
# e.g., ProgHomeDir libtoolize libtooldir.
ProgHomeDir() {
    prog=$1
    progpath="`which $prog`"
    progbindir="`dirname $progpath`"
    proghome=`(cd $progbindir/.. && pwd)`
    eval $2=$proghome
}

########################################################################
echo
echo "####################################"
echo "## Checking environment"
echo "####################################"
echo

########################################################################
## Checks to make sure we are running from the correct location
########################################################################

echo_n "Verifying the location of autogen.sh... "
if [ ! -d maint -o ! -s maint/version.m4 ] ; then
    echo "must execute at top level directory for now"
    exit 1
fi
# Set the SRCROOTDIR to be used later and avoid "cd ../../"-like usage.
SRCROOTDIR=$PWD
echo "done"

# Allow MAKE to be set from the environment
MAKE=${MAKE-make}

autoreconf_args="-ivf"
export autoreconf_args

########################################################################
## Check for the location of autotools
########################################################################

if [ -n "$autotoolsdir" ] ; then
    if [ -x $autotoolsdir/autoconf -a -x $autotoolsdir/autoheader ] ; then
        autoconf=$autotoolsdir/autoconf
        autoheader=$autotoolsdir/autoheader
        autoreconf=$autotoolsdir/autoreconf
        automake=$autotoolsdir/automake
        autom4te=$autotoolsdir/autom4te
        aclocal=$autotoolsdir/aclocal
        if [ -x "$autotoolsdir/glibtoolize" ] ; then
            libtoolize=$autotoolsdir/glibtoolize
        else
            libtoolize=$autotoolsdir/libtoolize
        fi

	AUTOCONF=$autoconf
	AUTOHEADER=$autoheader
        AUTORECONF=$autoreconf
        AUTOMAKE=$automake
	AUTOM4TE=$autom4te
        ACLOCAL=$aclocal
        LIBTOOLIZE=$libtoolize

	export AUTOCONF
	export AUTOHEADER
        export AUTORECONF
        export AUTOM4TE
        export AUTOMAKE
        export ACLOCAL
        export LIBTOOLIZE
    else
        echo "could not find executable autoconf and autoheader in $autotoolsdir"
	exit 1
    fi
else
    autoconf=${AUTOCONF:-autoconf}
    autoheader=${AUTOHEADER:-autoheader}
    autoreconf=${AUTORECONF:-autoreconf}
    autom4te=${AUTOM4TE:-autom4te}
    automake=${AUTOMAKE:-automake}
    aclocal=${ACLOCAL:-aclocal}
    if test -z "${LIBTOOLIZE+set}" && ( glibtoolize --version ) >/dev/null 2>&1 ; then
        libtoolize=glibtoolize
    else
        libtoolize=${LIBTOOLIZE:-libtoolize}
    fi
fi

ProgHomeDir $autoconf   autoconfdir
ProgHomeDir $automake   automakedir
ProgHomeDir $libtoolize libtooldir

echo_n "Checking if autotools are in the same location... "
if [ "$autoconfdir" = "$automakedir" -a "$autoconfdir" = "$libtooldir" ] ; then
    same_atdir=yes
    echo "yes, all in $autoconfdir"
else
    same_atdir=no
    echo "no"
    echo "	autoconf is in $autoconfdir"
    echo "	automake is in $automakedir"
    echo "	libtool  is in $libtooldir"
    # Emit a big warning message if $same_atdir = no.
    warn "Autotools are in different locations. In rare occasion,"
    warn "resulting configure or makefile may fail in some unexpected ways."
fi

########################################################################
## Verify autoconf version
########################################################################

echo_n "Checking for autoconf version... "
recreate_tmp
ver=2.67
# petsc.mcs.anl.gov's /usr/bin/autoreconf is version 2.65 which returns OK
# if configure.ac has AC_PREREQ() withOUT AC_INIT.
#
# ~/> hostname
# petsc
# ~> /usr/bin/autoconf --version
# autoconf (GNU Autoconf) 2.65
# ....
# ~/> cat configure.ac
# AC_PREREQ(2.68)
# ~/> /usr/bin/autoconf ; echo "rc=$?"
# configure.ac:1: error: Autoconf version 2.68 or higher is required
# configure.ac:1: the top level
# autom4te: /usr/bin/m4 failed with exit status: 63
# rc=63
# ~/> /usr/bin/autoreconf ; echo "rc=$?"
# rc=0
cat > .tmp/configure.ac<<EOF
AC_INIT
AC_PREREQ($ver)
AC_OUTPUT
EOF
if (cd .tmp && $autoreconf $autoreconf_args >/dev/null 2>&1 ) ; then
    echo ">= $ver"
else
    echo "bad autoconf installation"
    cat <<EOF
You either do not have autoconf in your path or it is too old (version
$ver or higher required). You may be able to use

     autoconf --version

Unfortunately, there is no standard format for the version output and
it changes between autotools versions.  In addition, some versions of
autoconf choose among many versions and provide incorrect output).
EOF
    exit 1
fi


########################################################################
## Verify automake version
########################################################################

echo_n "Checking for automake version... "
recreate_tmp
ver=1.15
cat > .tmp/configure.ac<<EOF
AC_INIT(testver,1.0)
AC_CONFIG_AUX_DIR([m4])
AC_CONFIG_MACRO_DIR([m4])
m4_ifdef([AM_INIT_AUTOMAKE],,[m4_fatal([AM_INIT_AUTOMAKE not defined])])
AM_INIT_AUTOMAKE([$ver foreign])
AC_MSG_RESULT([A message])
AC_OUTPUT([Makefile])
EOF
cat <<EOF >.tmp/Makefile.am
ACLOCAL_AMFLAGS = -I m4
EOF
if [ ! -d .tmp/m4 ] ; then mkdir .tmp/m4 >/dev/null 2>&1 ; fi
if (cd .tmp && $autoreconf $autoreconf_args >/dev/null 2>&1 ) ; then
    echo ">= $ver"
else
    echo "bad automake installation"
    cat <<EOF
You either do not have automake in your path or it is too old (version
$ver or higher required). You may be able to use

     automake --version

Unfortunately, there is no standard format for the version output and
it changes between autotools versions.  In addition, some versions of
autoconf choose among many versions and provide incorrect output).
EOF
    exit 1
fi


########################################################################
## Verify libtool version
########################################################################

echo_n "Checking for libtool version... "
recreate_tmp
ver=2.4.4
cat <<EOF >.tmp/configure.ac
AC_INIT(testver,1.0)
AC_CONFIG_AUX_DIR([m4])
AC_CONFIG_MACRO_DIR([m4])
m4_ifdef([LT_PREREQ],,[m4_fatal([LT_PREREQ not defined])])
LT_PREREQ($ver)
LT_INIT()
AC_MSG_RESULT([A message])
EOF
cat <<EOF >.tmp/Makefile.am
ACLOCAL_AMFLAGS = -I m4
EOF
if [ ! -d .tmp/m4 ] ; then mkdir .tmp/m4 >/dev/null 2>&1 ; fi
if (cd .tmp && $autoreconf $autoreconf_args >/dev/null 2>&1 ) ; then
    echo ">= $ver"
else
    echo "bad libtool installation"
    cat <<EOF
You either do not have libtool in your path or it is too old
(version $ver or higher required). You may be able to use

     libtool --version

Unfortunately, there is no standard format for the version output and
it changes between autotools versions.  In addition, some versions of
autoconf choose among many versions and provide incorrect output).
EOF
    exit 1
fi


########################################################################
## Generating configure
########################################################################
$autoreconf $autoreconf_args
