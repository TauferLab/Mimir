#!/bin/bash

DIST=""
DATALIST=()
PREFIX=""
OUTPUT=""

FPT=""
LANCHER="pbs"

if [ $# == 9 ]
then
  DIST=$1
  STDIV=$2
  PREFIX=$3
  OUTPUT=$4
  DATALIST=($5)
  NLIGANDS=($6)
  NFILELIST=($7)
  FPT=$8
  LANCHER=$9
else
  echo "./exe [dist] [stdiv] [prefix] [outdir] [data list] [ligands per file] [nfile list] [file per task] [lancher]"
fi

export DIST=$DIST
export PREFIX=$PREFIX
export STDIV=$STDIV

idx=0
for datasize in "${DATALIST[@]}"
do
  nfile=${NFILELIST[$idx]}
  export COUNT=${NLIGANDS[$idx]}
  echo "datasize",$datasize
  echo "nfile",$nfile
  mkdir -p $OUTPUT/$datasize
  #mkdir $OUTPUT/$datasize.points
  #export POINTDIR=$OUTPUT/$datasize.points/ 
  export OUTDIR=$OUTPUT/$datasize/
  ifile=0
  while [ $ifile -lt $nfile ];do
    lfile=$FPT
    let last=ifile+lfile
    if [ $last -gt $nfile ];then
      let lfile=nfile-ifile
    fi
    export SIDX=$ifile
    export NFILE=$lfile
    echo "offset",$ifile
    echo "lfile",$lfile
    if [ $LANCHER == "pbs" ];then
      qsub -V octreegenerator.pbs.sub 
    elif [ $LANCHER == "slurm" ];then
      sbatch octreegenerator.slurm.sub
    elif [ $LANCHER == "bgq" ]; then
#      python gen_key_point_rr.py $PREFIX $OUTDIR $SIDX $NFILE $STDIV  1 0 0 0 $COUNT $DIST
      python gen_key_point_rr.py $PREFIX $OUTDIR $SIDX $NFILE $STDIV  1 0 0 0 $COUNT $DIST &
#      ./bgq-wait-for-qlen.sh yguo 19
#      qsub -A MPICH_MCS -t 20 -n 1 --mode script \
#        ./octreegenerator.bgq.job $PREFIX $OUTDIR $SIDX $NFILE $STDIV $COUNT $DIST
    fi
    let ifile=ifile+lfile
  done
  let idx+=1
done
