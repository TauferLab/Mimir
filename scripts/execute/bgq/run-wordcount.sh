#! /bin/zsh

if [ $# -lt 2 ]; then
    echo "./run-wordcount.sh <data size in MB> [n_node start] [n_node end] [uniform/wikipedia]"
    exit 1
fi

export  INPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data
export OUTPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data

WC_TYPE=${4:-"uniform"}

function run() {
    local N=$1
    local n_start=$2
    local n_end=$3
    if [ $n_end -lt $n_start ]; then
        n_end=$n_start
    fi
    local timeout=${4:-45}
    local sizename="$1M"

    if [ $N -gt 1024 ]; then
        sizename="$(($N/1024))G"
    fi

    local input_dir_name="$WC_TYPE/weakscale$sizename"
    if [ $n_start -eq 1 ]; then
        input_dir_name="$WC_TYPE/singlenode/$sizename"
    fi

    for ((i = n_start; i <= n_end; i = i * 2)); do
        param=""
        ./sub_jobs.sh basic       wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cps         wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh pr          wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh kvhint      wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cpskvhint   wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh prkvhint    wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
        ./sub_jobs.sh cpsprkvhint wordcount $input_dir_name 5 $i $i 512M 64M $timeout $param
    done
}

run $1 ${2:-1} ${3:-1}
