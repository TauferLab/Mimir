#!/bin/bash
./sub_jobs.sh bfs "basic cb kvhint cbkvhint" \
    graph500 "singlenode" \
    "s20 s21 s22 s23 s24 s25 s26 s27" "1048576 2097152 4194304 8388608 16777216 33554432 67108864 134217728" \
    5 1 "bfs_graph500_singlenode_c64M-p64M-i512M-h20" $1
