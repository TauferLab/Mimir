#!/bin/bash
./sub_jobs.sh wordcount "basic cb kvhint cbkvhint" \
    wikipedia weakscale4G "8G 16G 32G 64G 128G 256G" "" \
    5 2 "wc_wikipedia_weakscale4G_c64M-p64M-i512M-h20" none
