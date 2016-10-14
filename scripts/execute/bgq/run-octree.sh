#! /bin/zsh

if [ $# -lt 2 ]; then
    echo "./run-octree.sh <data size in MB> [n_node start] [n_node end]"
    exit 1
fi

export  INPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data
export OUTPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data

function run() {
    local N=$1
    local n_start=$2
    local n_end=$3
    if [ $n_end -lt $n_start ]; then
        n_end=$n_start
    fi
    local timeout=${4:-45}

    local input_dir_name=weakscale$1
    if [ $n_start -eq 1 ]; then
        input_dir_name="singlenode/$1"
    fi

    for ((i = n_start; i <= n_end; i = i * 2)); do
        param="0.01"
        ./sub_jobs.sh basic       octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cps         octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh pr          octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh kvhint      octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cpskvhint   octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh prkvhint    octree $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cpsprkvhint octree $input_dir_name 5 $i $i 512M 64M $timeout $param
    done
}

run $1 ${2:-1} ${3:-1}
