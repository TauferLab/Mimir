python compare_time_breakdown.py \
  "mrmpi-p64-octree-1S-onenode_d0.01-a2a.ppn24_phases.txt"\
  "MR-MPI(64M page size)"\
  "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" \
  onenode-mrmpi-octree-1S-p64M-breakdown 24 1 --plottype point --datatype mean --normalize false --ylim 0 5000

python compare_time_breakdown.py \
  "mrmpi-p64-octree-1S-onenode_d0.01-a2a.ppn24_phases.txt"\
  "MR-MPI(512M page size)"\
  "1G,2G,4G,8G,16G,32G,64G,128G,256G,512G,1T,2T" \
  onenode-mrmpi-octree-1S-p512M-breakdown 24 1 --plottype point --datatype mean --normalize false --ylim 0 5000

#python compare_breakdown_onenode.py \
#  "mtmrmpi-octree-basic_onenode_1S-d0.01-l4K-c64M-b64M-i512M-h17-a2a.ppn24_phases.txt"\
#  "MR-MPI++"\
#  "32M,64M,128M,256M,512M,1G,2G,4G,8G,16G,32G,64G,128G,256G,512G" \
#  onenode-mtmrmpi-basic-octree-1S-breakdown 24 1 --plottype point --ylim 0 2000
