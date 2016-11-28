#! /bin/zsh

if [ $# -lt 2 ]; then
    echo "./run-bfs.sh <data size in MB> [n_node start] [n_node end]"
    exit 1
fi

export  INPUT_BASEDIR=/projects/SSSPPg/yguo/mimir/data
export OUTPUT_BASEDIR=/projects/SSSPPg/yguo/mimir/data

function run() {
    local N=$(echo "2^$1" | bc)
    local n_start=$2
    local n_end=$3
    if [ $n_end -lt $n_start ]; then
        n_end=$n_start
    fi
    local timeout=${4:-45}

    local input_dir_name="1S/weakscale$1"
    if [ $n_start -eq 1 ]; then
        input_dir_name="1S/singlenode/s$1"
    fi

    for ((i = n_start; i <= n_end; i = i * 2)); do
        param=$(($N*$i))
        ./sub_jobs.sh basic     bfs $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cb        bfs $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh kvhint    bfs $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cbkvhint  bfs $input_dir_name 5 $i $i 512M 64M $timeout $param
    done
}

run $1 ${2:-1} ${3:-1}
