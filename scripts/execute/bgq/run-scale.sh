#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"
dataset="wikipedia"

function create_data_M {
    pushd ../../gen-bgq
    for node in $(echo 128); do
        ./sub_split.sh "$BDIR/wordcount/wikipedia/wikipedia_300GB" words \
            "$BDIR/wordcount/wikipedia/weakscale$1M" \
            "$node" \
            "$(($1*1024*1024/16))" \
            "$((16*$node))" 16 bgq
    done
    popd
}

function create_data_G {
    pushd ../../gen-bgq
    for node in $(echo 64 128); do
        ./sub_split.sh "$BDIR/wordcount/wikipedia/wikipedia_300GB" words \
            "$BDIR/wordcount/wikipedia/weakscale$1G" \
            "$node" \
            "$(($1*1024*1024*1024/16))" \
            "$((16*$node))" 16 bgq
    done
    popd
}

function run_size {
    date
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh basic wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh cps wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh pr wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh kvhint wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh cpskvhint wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh prkvhint wordcount $dataset/weakscale$1 5 64 128 256M
    ./bgq-wait-for-qlen.sh yguo 18
    ./sub_jobs-scale.sh cpsprkvhint wordcount $dataset/weakscale$1 5 64 128 256M
    date
}

function clear_data {
    rm -rf "$BDIR/tmp/*"
    rm -rf "$BDIR/wordcount/wikipedia/weakscale$1"
}

#create_data_M 32
#create_data_M 64
#create_data_M 128

#run_size 32M
#run_size 64M
#run_size 128M

#clear_data 32M
#clear_data 64M
#clear_data 128M

#create_data_M 256
#run_size 256M
#create_data_M 512
#run_size 512M
#clear_data 256M
#clear_data 512M

#run_size 512M
#create_data_M 1024
#run_size 1024M
#clear_data 1024M
#create_data_G 2
run_size 2G
#run_size 4G
#run_size 8G

#./sub_jobs-scale.sh basic wordcount wikipedia/weakscale256M 5 128 128 256M
#./sub_jobs-scale.sh basic wordcount wikipedia/weakscale512M 5 64 128 256M
#./sub_jobs-scale.sh pr wordcount wikipedia/weakscale256M 5 128 128 256M
#./sub_jobs-scale.sh pr wordcount wikipedia/weakscale512M 5 128 128 256M


exit 0
#./sub_jobs.sh basic wordcount wikipedia/weakscale32M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale64M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale128M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale1024M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh basic wordcount wikipedia/weakscale2G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13

date
exit 0

#./sub_jobs.sh cps wordcount wikipedia/weakscale32M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale64M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale128M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale1024M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale2G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale4G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cps wordcount wikipedia/weakscale8G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#
#./sub_jobs.sh pr wordcount wikipedia/weakscale32M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale64M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale128M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale1024M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh pr wordcount wikipedia/weakscale2G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale32M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale64M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale128M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale1024M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale2G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale4G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh cpskvhint wordcount wikipedia/weakscale8G 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale32M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale64M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale128M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale256M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale1024M 5 2 128 256M
#./bgq-wait-for-qlen.sh yguo 13
#./sub_jobs.sh prkvhint wordcount wikipedia/weakscale2G 5 2 128 256M
