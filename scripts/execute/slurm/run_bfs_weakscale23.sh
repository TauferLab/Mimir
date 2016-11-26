#!/bin/bash
./sub_jobs.sh bfs "basic cb kvhint cbkvhint" \
    graph500 "weakscale23" \
    "s24 s25 s26 s27 s28 s29" "16777216 33554432 67108864 134217728 268435456 536870912" \
    5 2 "bfs_graph500_weakscale23_c64M-p64M-i512M-h20" none
