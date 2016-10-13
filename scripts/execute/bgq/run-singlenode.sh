#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

function create_data_M {
    pushd ../../gen-bgq
    for node in $(echo 1); do
        ./sub_split.sh "$BDIR/wordcount/wikipedia/wikipedia_300GB" words \
            "$BDIR/wordcount/wikipedia/singlenode/$1M" \
            "$node" \
            "$(($1*1024*1024/16))" \
            "$((16*$node))" 16 bgq
    done
    popd
}

function create_data_G {
    pushd ../../gen-bgq
    for node in $(echo 1); do
        ./sub_split.sh "$BDIR/wordcount/wikipedia/wikipedia_300GB" words \
            "$BDIR/wordcount/wikipedia/singlenode/$1G" \
            "$node" \
            "$(($1*1024*1024*1024/16))" \
            "$((16*$node))" 16 bgq
    done
    popd
}

function run_size {
    date
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh basic wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh cps wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh pr wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh kvhint wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh cpskvhint wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh prkvhint wordcount uniform/singlenode/$1 5 1 1 512M
    ./bgq-wait-for-qlen.sh yguo 19
    ./sub_jobs.sh cpsprkvhint wordcount uniform/singlenode/$1 5 1 1 512M
    date
}

#run_size 32M
#run_size 64M
#run_size 128M
#run_size 256M
#run_size 512M
run_size 1024M
run_size 2G
run_size 4G
run_size 8G
#run_size 16G
#run_size 32G

exit 0

