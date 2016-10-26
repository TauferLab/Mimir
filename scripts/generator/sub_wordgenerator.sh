#!/bin/bash

export PREFIX=words

DIST=""
DATALIST=()
PREFIX=""
OUTPUT=""
FSIZEMAX=1073741824
FPT=""
LANCHER="pbs"
WORDS=100000

if [ $# == 9 ]
then
  DIST=$1
  WORDS=$2
  PREFIX=$3
  OUTPUT=$4
  DATALIST=($5)
  FSIZELIST=($6)
  NFILELIST=($7)
  FPT=$8
  LANCHER=$9
else
  echo "./exe [distribution] [nunique] [prefix] [outdir] [data list] [fsize list] [nfile list] [file per task] [lancher]"
fi

export UNIQUE=$WORDS
export DIST=$DIST
export PREFIX=$PREFIX

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
  mkdir $OUTPUT/$datasize
  export FSIZE=$fsize
  export OUTDIR=$OUTPUT/$datasize
  ifile=0
  while [ $ifile -lt $nfile ];do
    lfile=$FPT
    let last=ifile+lfile
    if [ $last -gt $nfile ];then
      let lfile=nfile-ifile
    fi
    export OFFSET=$ifile
    export NFILE=$lfile
    echo "offset",$ifile
    echo "lfile",$lfile
    if [ $LANCHER == "pbs" ];then
      qsub -V wordgenerator.pbs.sub 
    elif [ $LANCHER == "slurm" ];then
      sbatch wordgenerator.slurm.sub
    fi
    let ifile=ifile+lfile
  done
  let idx+=1
done
