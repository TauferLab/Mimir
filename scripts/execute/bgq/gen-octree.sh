#! /bin/zsh

source bgq-common.sh
source retry.sh

if [ $# -lt 2 ]l then
    echo "./run-octree.sh <data size in MB> [n_node start] [n_node end]"
    exit 1
fi

export  INPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data
export OUTPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data

function run() {
    local N=$1
    local N_LIGAND=$(echo "2^$1" | bc)
    local n_start=$2
    local n_end=$3
    if [ $n_end -lt $n_start ]; then
        n_end=$n_start
    fi
    local timeout=${4:-45}

    local input_dir_name=weakscale$N
    if [ $n_start -eq 1 ]; then
        input_dir_name="singlenode/$N"
    fi

    pushd ../../gen-bgq
    for ((i = n_start; i <= n_end; i = i * 2)); do
        timeout=$(get_timeout $i $timeout)
        retry 10m \
            qsub -A MPICH_MCS -t $timeout -n $i --mode script \
                ./octreegenerator.bgq.job octree \
                    "$INPUT_BASEDIR/octree/$input_dir_name/$i" \
                    0 0 0.5 "$N_LIGAND" normal $i
        echo "  => octree scale-per-proc $N for $i node"
    done
    popd
}

run $1 ${2:-1} ${3:-1}
