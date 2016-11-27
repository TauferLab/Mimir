#!/bin/bash
./sub_jobs.sh octree "basic cb kvhint cbkvhint" \
    1S weakscale28 "p29 p30 p31 p32 p33 p34" "0.01 0.01 0.01 0.01 0.01 0.01" \
    5 2 "octree_1S_weakscale28_c64M-p64M-i512M-h20" $1
