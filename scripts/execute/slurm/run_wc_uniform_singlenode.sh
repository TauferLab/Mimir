#!/bin/bash
./sub_jobs.sh wordcount "basic cb kvhint cbkvhint" \
    uniform singlenode "32M 64M 128M 256M 512M 1G 2G 4G 8G 16G 32G 64G" "" \
    5 1 "wc_uniform_singlenode_c64M-p64M-i512M-h20" $1
