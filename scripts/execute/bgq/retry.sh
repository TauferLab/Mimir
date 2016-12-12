function retry() {
    local timeout=$1
    local cmd=$2

#    $cmd ${@:2}
    ${@:2}
    while [ $? -ne 0 ]; do
        sleep $timeout
#        $cmd ${@:2}
        ${@:2}
    done
}
