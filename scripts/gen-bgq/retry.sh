#! /bin/zsh

# retry after sleep
# retry [sleep_time] [command] [args]

function retry {
    local time=$1
    local cmd=$2
    $cmd ${@: 2}
    while [ $? -ne 0 ]; do
        echo "retry in $time"
        sleep $time
        $cmd ${@: 2}
    done
}

