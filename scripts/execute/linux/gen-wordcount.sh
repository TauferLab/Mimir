#! /bin/zsh

source bgq-common.sh
source retry.sh

if [ $# -lt 2 ]; then
    echo "./run-wordcount.sh <data size in MB> [n_node start] [n_node end] [uniform/wikipedia]"
    exit 1
fi

export  INPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data
export OUTPUT_BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data

WC_TYPE=${4:-"uniform"}

function run() {
    local N=$1
    local FSIZE=$(($N*1024*1024/16))
    local n_start=$2
    local n_end=$3
    if [ $n_end -lt $n_start ]; then
        n_end=$n_start
    fi
    local timeout=${4:-15}
    local sizename="$1M"

    if [ $N -gt 1024 ]; then
        sizename="$(($N/1024))G"
    fi

    local input_dir_name="$WC_TYPE/weakscale$sizename"
    if [ $n_start -eq 1 ]; then
        input_dir_name="$WC_TYPE/singlenode/$sizename"
    fi

    pushd ../../gen-bgq
    for ((i = n_start; i <= n_end; i = i * 2)); do
        timeout=$(get_timeout $i $timeout)
        retry 5m \
            qsub -A MPICH_MCS -t $timeout -n $i --mode script \
                ./wordcount-uniform.bgq.job wordcount \
                "$INPUT_BASEDIR/wordcount/$input_dir_name/$i" \
                $FSIZE 100000 $i
        echo "  => wordcount $WC_TYPE filesize $N MB for $i node"
    done
    popd
}

run $1 ${2:-1} ${3:-1}
