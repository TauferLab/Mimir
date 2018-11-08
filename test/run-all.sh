#!/bin/bash -e
#
# (c) 2018 by The University of Tennessee Knoxville, Argonne National
#     Laboratory, San Diego Supercomputer Center, National University of
#     Defense Technology, National Supercomputer Center in Guangzhou,
#     and Sun Yat-sen University.
#
#     See COPYRIGHT in top-level directory.

./check-bfs-join.sh
./check-bfs.sh
./check-join-split.sh
./check-join.sh
./check-oc-cb.sh
./check-oc.sh
./check-wc-cb.sh
./check-wc.sh
