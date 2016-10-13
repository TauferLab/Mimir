#! /bin/zsh

function run_size {
    date
    N=$(echo "2^$1" | bc)
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_bfs_jobs-scale.sh basic bfs graph500_weakscale$1 5 64 128 256M $N
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_bfs_jobs-scale.sh cps bfs graph500_weakscale$1 5 64 128 256M $N
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_bfs_jobs-scale.sh kvhint bfs graph500_weakscale$1 5 64 128 256M $N
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_bfs_jobs-scale.sh cpskvhint bfs graph500_weakscale$1 5 64 128 256M $N
    date
}

#run_size 16
#run_size 17
#run_size 18
#run_size 19
#run_size 20
#run_size 21
run_size 22
#run_size 23
#run_size 24
#run_size 25

exit 0

#.sub_jobs.sh basic wordcount wikipedia/singlenode/32M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/64M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/128M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/1024M 5 1 1 256M
#./sub_jobs.sh basic wordcount wikipedia/singlenode/2G 5 1 1 256M
#
#./bgq-wait-for-qlen.sh yguo 11
#
#./sub_jobs.sh cps wordcount wikipedia/singlenode/32M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/64M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/128M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/1024M 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/2G 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/4G 5 1 1 256M
#./sub_jobs.sh cps wordcount wikipedia/singlenode/8G 5 1 1 256M
#
#./bgq-wait-for-qlen.sh yguo 13
#
#./sub_jobs.sh pr wordcount wikipedia/singlenode/32M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/64M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/128M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/1024M 5 1 1 256M
#./sub_jobs.sh pr wordcount wikipedia/singlenode/2G 5 1 1 256M
#
#./bgq-wait-for-qlen.sh yguo 11
#
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/32M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/64M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/128M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/1024M 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/2G 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/4G 5 1 1 256M
#./sub_jobs.sh cpskvhint wordcount wikipedia/singlenode/8G 5 1 1 256M
#
#./bgq-wait-for-qlen.sh yguo 13
#
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/32M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/64M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/128M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/256M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/1024M 5 1 1 256M
#./sub_jobs.sh prkvhint wordcount wikipedia/singlenode/2G 5 1 1 256M
