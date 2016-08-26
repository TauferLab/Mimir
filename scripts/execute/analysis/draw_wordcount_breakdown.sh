#!/bin/bash

#benchmark="wordcount"
#datasets="wikipedia"

#python compare_time_breakdown.py \
#  "mtmrmpi-partreducekvhint-wordcount-wikipedia-onenode_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
#  "MR-MPI++(partial-reduction;KV-hint)"\
#  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" \
#  onenode-mtmrmpi-wordcount-wikipedia-breakdown 24 1 --plottype point --datatype median --normalize false --ylim 0 400

python compare_time_breakdown.py \
  "mtmrmpi-ptkvnb-wordcount-uniform-weekscale4G_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
  "MR-MPI++"\
  "4G,8G,16G,32G,64G,128G,256G" \
  onenode-mtmrmpi-wordcount-uniform-breakdown 24 1 --plottype point --datatype max --normalize false --ylim 0 100

#python compare_time_breakdown.py \
#  "mrmpi-octree-onenode_1S-d0.01-p512M-a2a.ppn24_phases.txt"\
#  "MR-MPI(512M page size)"\
#  "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" \
#  onenode-mrmpi-octree-1S-p512M-breakdown 24 1 --plottype point --datatype mean --normalize false --ylim 0 5000

