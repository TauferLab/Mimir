#./sub_by_dataset.sh bfs "basic partreduce kvhint" "graph500" onenode \
#  "32M      64M   128M   256M    512M       1G      2G      4G       8G      16G      32G" \
#  "65536 131072 262144 524288 1048576  2097152 4194304 8388608 16777216 33554432 67108864"  \
#  5 run.onenode.sub none

./sub_by_dataset.sh bfs "kvhint" "graph500" onenode \
  "64G" \
  "134217728"  \
  5 run.onenode.sub none
