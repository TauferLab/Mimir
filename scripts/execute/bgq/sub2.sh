#! /bin/zsh

# ./sub_jobs.sh wordcount "basic partreduce partreducekvhint" \
#   "uniform" "weakscale_small" \
#   "32M" \
#   "" 1 none

#./sub_jobs.sh basic wordcount uniform/weakscale_small/32M 2 1 4 "no param"
./sub_jobs2.sh basic wordcount uniform/weakscale_small 1 1 1 "" "32M 64M 128M 256M 512M 1024M"
