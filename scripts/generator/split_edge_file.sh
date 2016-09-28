#!/bin/bash
#SBATCH --job-name="cp"  
#SBATCH --output="cp.%j.%N.out"  
#SBATCH --partition=debug 
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=24
#SBATCH --export=ALL  
#SBATCH -t 00:30:00 

SCALE=$1
edgefactor=$2
INDIR=$3
OUTDIR=$4
START=$5
END=$6
NSPLIT=$7
IDX=$8

for((j=$START; j<$END; j+=1));do
  cat $INDIR/$SCALE.$edgefactor.$j.txt >> tmp.$IDX.txt
done

nline=$(wc -l tmp.$IDX.txt | awk '{print $1}')
let lpf=nline/NSPLIT
if [ $nline%$NSPLIT != 0 ];then
  let lpf=lpf+1
fi
split -l $lpf tmp.$IDX.txt $OUTDIR/$SCALE.$edgefactor.$IDX.txt
rm tmp.$IDX.txt
