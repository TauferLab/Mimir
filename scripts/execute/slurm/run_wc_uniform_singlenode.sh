#!/bin/bash
./sub_jobs.sh wordcount "basic cb kvhint cbkvhint" \
    uniform singlenode "1G 2G 4G 8G 16G 32G 64G" "" \
    1 1 "wc_uniform_singlenode_c64M-p64M-i512M-h17" $1
