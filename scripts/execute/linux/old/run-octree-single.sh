#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

# size == 15 is 32M/node

function create_data {
    pushd ../../gen-bgq
    N_LIGAND=$(echo "2^$1" | bc)
    for node in $(echo 1); do
        ./sub_octreegenerator.sh normal 0.5 octree \
            "$BDIR/octree/singlenode" \
            $1 "$N_LIGAND" \
            16 16 bgq
    done
    popd
}

function create_data_mn {
    pushd ../../gen-bgq
    N_LIGAND=$(echo "2^$1" | bc)
    for node in $(echo $2); do
        ./sub_octreegenerator.sh normal 0.5 octree \
            "$BDIR/octree/weakscale$1/$node" \
            $1 "$N_LIGAND" \
            $(($node*16)) 16 bgq
    done
    popd
}

#create_data_mn 21 2
#create_data_mn 21 4

function clear_data {
    rm -rf "$BDIR/octree/singlenode/$1"
}

function run_size {
    date
    N="0.01"
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_jobs.sh basic octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh cps octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh pr octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh kvhint octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh cpskvhint octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh prkvhint octree singlenode/$1 5 1 1 256M $N
    ./sub_jobs.sh cpsprkvhint octree singlenode/$1 5 1 1 256M $N
    date
}

#create_data 15
#create_data 16
#create_data 17
#create_data 18
#create_data 19
#create_data 20
#create_data 21
#create_data 22
#create_data 23
#create_data 24
#create_data 25
#create_data 26

#run_size 15
#run_size 16
#run_size 17
#run_size 18
#run_size 19
run_size 20
run_size 21
run_size 22
run_size 23
run_size 24
run_size 25
run_size 26

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
