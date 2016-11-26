#!/bin/bash
./sub_jobs.sh bfs "basic cb kvhint cbkvhint" \
    graph500 "weakscale20" \
    "s21 s22 s23 s24 s25 s26" "2097152 4194304 8388608 16777216 33554432 67108864" \
    5 2 none
