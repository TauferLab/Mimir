#!/bin/bash

export THRS=1
FSIZEMAX=1073741824

INDIR=""
PREFIX=""
OUTDIR=""
DATALIST=()
FSIZELIST=()
NFILELIST=()
FPT=""
LANCHER="pbs"

if [ $# == 8 ]
then
  INDIR=$1
  PREFIX=$2
  OUTDIR=$3
  DATALIST=($4)
  FSIZELIST=($5)
  NFILELIST=($6)
  FPT=$7
  LANCHER=$8
else
  echo "./exe [indir] [prefix] [outdir] [data list] [fsize list] [nfile list] [file per task] [lancher]"
fi

export INPUT=$INDIR
export PREFIX=$PREFIX

echo ${DATALIST[@]}

idx=0
for datasize in "${DATALIST[@]}"
do
  fsize=${FSIZELIST[$idx]}
  nfile=${NFILELIST[$idx]}
  #let sizepfile=fsize/nfile
  #let ntimes=$sizepfile/$FSIZEMAX 
  #if [ $ntimes != 0 ];then
  #  let ntimes+=1
  #  let sizepfile=sizepfile/ntimes
  #  let nfile=nfile*ntimes
  #fi
  echo "datasize",$datasize
  echo "fsize",$fsize
  echo "nfile",$nfile
  #echo "sizepfile",$sizepfile
  mkdir $OUTDIR/$datasize
  export FSIZE=$fsize
  export OUTPUT=$OUTDIR/$datasize
  ifile=0
  #while [ $ifile -lt $nfile ];do
  #  lfile=$FPT
  #  let last=ifile+lfile
  #  if [ $last -gt $nfile ];then
  #    let lfile=nfile-ifile
  #  fi
  #  export SIDX=$ifile
  #  export NFILE=$lfile
  #  echo "offset",$ifile
  #  echo "lfile",$lfile
  export SIDX=0
  export NFILE=$nfile
  #echo "nfile"
    if [ $LANCHER == "pbs" ];then
      qsub -V split.pbs.sub 
    elif [ $LANCHER == "slurm" ];then
      sbatch split.slurm.sub
    fi
    let ifile=ifile+lfile
  #done
  let idx+=1
done
