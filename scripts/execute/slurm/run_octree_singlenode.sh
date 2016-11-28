#!/bin/bash
./sub_jobs.sh octree "basic cb kvhint cbkvhint" \
    1S singlenode "p24 p25 p26 p27 p28 p29 p30 p31 p32" "0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.01 0.01" \
    5 1 "octree_1S_singlenode_c64M-p64M-i512M-h20" $1
