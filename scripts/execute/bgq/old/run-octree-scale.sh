#! /bin/zsh

BDIR="/projects/MPICH_MCS/yguo/mt-mrmpi/data"

function create_data {
    pushd ../../gen-bgq
    N_LIGAND=$(echo "2^$1" | bc)
    for node in $(echo 16); do
        ./sub_octreegenerator.sh normal 0.5 octree \
            "$BDIR/octree/weakscale$1/$node" \
            $1 "$N_LIGAND" \
            $(($node*16)) 16 bgq
    done
    popd
}

function run_size {
    date
    N="0.01"
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh basic octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh cps octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh pr octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh kvhint octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh cpskvhint octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh prkvhint octree weakscale$1 5 2 128 512M $N
    ./bgq-wait-for-qlen.sh yguo 13
    ./sub_octree_jobs-scale.sh cpsprkvhint octree weakscale$1 5 2 128 512M $N
    date
}

function clear_data {
    rm -rf "$BDIR/octree/weakscale$1"
}

#create_data 15
#run_size 15
#clear_data 15

#create_data 16
#run_size 16
#clear_data 16
#
#create_data 17
#run_size 17
#clear_data 17
#
#create_data 18
#run_size 18
#clear_data 18
#
#create_data 19
#run_size 19
#clear_data 19

#create_data 20
#run_size 20
#clear_data 20

#create_data 21
#run_size 21
#./sub_octree_jobs-scale.sh cpsprkvhint octree weakscale21 5 128 128 256M 0.01
#clear_data 21

#create_data 22
#run_size 22
#clear_data 22

#create_data 23
run_size 23
#clear_data 23

#run_size 17
#run_size 18
#run_size 19
#run_size 20
#run_size 21
#run_size 22
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
