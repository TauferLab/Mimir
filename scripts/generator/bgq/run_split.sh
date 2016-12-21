#!/bin/bash
BASEDIR=/projects/SSSPPg/yguo/mt-mrmpi/data
datasets="wikipedia"

# ./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
#   "$BASEDIR/wordcount/wikipedia/singlenode" \
#   "32M" \
#   "$((32/16*1024*1024))" \
#   "16" 16 bgq
#./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
#  "$BASEDIR/wordcount/wikipedia/singlenode" \
#  "64M 128M 256M 512M 1024M 2G 4G 8G" \
#  "$((64/16*1024*1024)) $((128/16*1024*1024)) $((256/16*1024*1024)) $((512/16*1024*1024)) $((1024/16*1024*1024)) $(2048/16*1024*1024)) $((4096/16*1024*1024)) $((8*1024*1024*1024/16))" \
#  "16 16 16 16 16 16 16 16" 16 bgq

./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_300GB" words \
  "$BASEDIR/wordcount/wikipedia/singlenode" \
  "32M 64M 128M 256M 512M 1024M 2G 4G 8G" \
  "$((32/16*1024*1024)) $((64/16*1024*1024)) $((128/16*1024*1024)) $((256/16*1024*1024)) $((512/16*1024*1024)) $((1024/16*1024*1024)) $((2048/16*1024*1024)) $((4096/16*1024*1024)) $((8*1024*1024*1024/16))" \
  "16 16 16 16 16 16 16 16 16" 16 bgq
#./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
#  "$BASEDIR/wordcount/wikipedia/singlenode" \
#  "512M 1024M 2G 4G 8G" \
#  "$((512/16*1024*1024)) $((1*1024*1024*1024/16)) $((2*1024*1024*1024/16)) $((4*1024*1024*1024/16)) $((8*1024*1024*1024/16))" \
#  "16 16 16 16 16" 16 bgq

#./sub_split.sh "$BASEDIR/wordcount/wikipedia/wikipedia_50GB" words \
#  "$BASEDIR/wordcount/wikipedia/weakscale64M" \
#  "2 4 8 16 32 64 128" \
#  "$((64/16*1024*1024)) $((64/16*1024*1024)) $((64/16*1024*1024)) $((64/16*1024*1024)) $((64/16*1024*1024)) $(64/16*1024*1024)) $((64/16*1024*1024))" \
#  "32 64 128 256 512 1024 2048" 16 bgq
