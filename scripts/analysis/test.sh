BASEDIR=/usa/taogao/repo/gclab/projects/mimir/data

python compare-mrmpi-time.py octree 1S \
    weakscale25 p26,p27,p28,p29,p30,p31 p32,p32,p32\
    "2^26,2^27,2^28,2^29,2^30,2^31" \
    --indir1 $BASEDIR/comet/octree_1S_weakscale25_c64M-p64M-i512M-h20/ \
    --indir2  $BASEDIR/mrmpi/comet/ \
    --outfile comet-weakscale25-octree-1S-time.pdf \
    --settings1  "basic" \
    --settings2  "p64,p512" \
    --labellist1 "Mimir (basic)" \
    --labellist2 "MR-MPI (64M),MR-MPI (512M)" \
    --colorlist "darkviolet,red,blue" \
    --hatches "x+/" \
    --markerlist "*,^,v" \
    --xlabelname "number of points" \
    --ylim -20 150

