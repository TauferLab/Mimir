#! /bin/zsh

# ./sub_jobs.sh wordcount "basic partreduce partreducekvhint" \
#   "uniform" "weakscale_small" \
#   "32M" \
#   "" 1 none

#./sub_jobs.sh basic wordcount uniform/weakscale_small/32M 2 1 4 "no param"
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale32M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale64M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale128M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale256M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale512M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale1024M 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale2G 5 2 128 256M
./bgq-wait-for-qlen.sh yguo 13
./sub_jobs-scale.sh cpskvhint wordcount uniform/weakscale4G 5 2 128 256M
