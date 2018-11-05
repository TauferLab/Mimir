[#] start of __file__
#
# (c) 2018 by The University of Tennessee Knoxville, Argonne National
#     Laboratory, San Diego Supercomputer Center, National University of
#     Defense Technology, National Supercomputer Center in Guangzhou,
#     and Sun Yat-sen University.
#
#     See COPYRIGHT in top-level directory.
#
# This file contains versioning information for Mimir's configure process.
#
# !!! NOTE !!! absolutely no shell code from this file will end up in the
# configure script, including these shell comments.  Any shell code must live in
# the configure script and/or use m4 values defined here.  We could consider
# changing this by playing with diversions, but then we would probably be
# playing with autotools-fire.

m4_define([MIMIR_VERSION_m4],[0.9])dnl
m4_define([MIMIR_RELEASE_DATE_m4],[unreleased development copy])dnl

m4_define([libmimir_so_version_m4],[0:0:0])dnl

[#] end of __file__

