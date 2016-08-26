#!/bin/bash
benchmark="bfs"
datasets="graph500"

for dataset in $datasets
do
  python compare_total_time.py \
    "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size)"\
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "32G,32G"\
  comet-onenode-$benchmark-$dataset-original-time 24 1 --indir ../data/comet/ --plottype bar --xtickettype dataset --ylim 0 1000
done

for dataset in $datasets
do
  python compare_total_time.py \
    "mrmpi-p64-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-onenode_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreduce-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreducekvhint-$benchmark-$dataset-onenode_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G" "1G,8G,32G,64G,128G"\
  comet-onenode-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --ylim 0 200
done

for dataset in $datasets
do
  python compare_total_time.py \
    "mrmpi-p64-$benchmark-$dataset-weekscale4G_a2a.ppn24_phases.txt,\
mrmpi-p512-$benchmark-$dataset-weekscale4G_a2a.ppn24_phases.txt"\
    "MR-MPI(64M page size),\
MR-MPI(512M page size)"\
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G"\
  comet-weekscale4G-$benchmark-$dataset-time 24 1 --indir ../data/comet/ --plottype bar --xtickettype node  --ylim 0 600
done

for dataset in $datasets
do
  python compare_total_time.py \
    "mrmpi-p512-$benchmark-$dataset-weekscale4G_a2a.ppn24_phases.txt,\
mtmrmpi-basic-$benchmark-$dataset-weekscale4G_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreduce-$benchmark-$dataset-weekscale4G_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt,\
mtmrmpi-partreducekvhint-$benchmark-$dataset-weekscale4G_c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
    "MR-MPI(512M page size),\
MR-MPI++,\
MR-MPI++(partial-reduction),\
MR-MPI++(partial-reduction;KV-hint)" \
  "4G,8G,16G,32G,64G,128G,256G" "512G,512G,512G,512G,512G"\
  comet-weekscale4G-$benchmark-$dataset-inmem-time 24 1 --indir ../data/comet/ --xtickettype node  --plottype bar --ylim 0 100 --coloroff 1
done
