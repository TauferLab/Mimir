#! /bin/zsh

if [ $# -lt 2 ]; then
    echo "./bgq-wait-for-qlen.sh <USERNAME> <Queue Length Threshold>"
    exit 1
fi

USER=$1
QLEN_THRESHOLD=$2

LINES=$(($(qstat -u $USERNAME | wc -l) - 2))

while [ $LINES -ge $QLEN_THRESHOLD ]; do
    echo "$LINES jobs in queue, waiting..."
    sleep 2m
    LINES=$(($(qstat -u $USERNAME | wc -l) - 2))
done

exit 0
