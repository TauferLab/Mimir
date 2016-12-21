function get_timeout() {
    local n_nodes=$1
    if [ $n_nodes -gt 2048 ]; then
        echo "60"
    else
        echo $2
    fi
}
