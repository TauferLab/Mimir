#!/bin/bash

export UNIQUE=100000
export PREFIX=words

DIST=""
DATALIST=()
PREFIX=""
OUTPUT=""
FSIZEMAX=1073741824
FPT=""

if [ $# == 7 ]
then
  DIST=$1
  PREFIX=$2
  OUTPUT=$3
  DATALIST=($4)
  FSIZELIST=($5)
  NFILELIST=($6)
  FPT=$7
else
  echo "./exe [distribution] [prefix] [outdir] [data list] [fsize list] [nfile list] [file per task]"
fi

export DIST=$DIST
export PREFIX=$PREFIX

idx=0
for datasize in "${DATALIST[@]}"
do
  fsize=${FSIZELIST[$idx]}
  nfile=${NFILELIST[$idx]}
  let sizepfile=fsize/nfile
  let ntimes=$sizepfile/$FSIZEMAX 
  if [ $ntimes != 0 ];then
    let ntimes+=1
    let sizepfile=sizepfile/ntimes
    let nfile=nfile*ntimes
  fi
  echo "datasize",$datasize
  echo "fsize",$fsize
  echo "nfile",$nfile
  echo "sizepfile",$sizepfile
  mkdir $OUTPUT/$datasize
  export FSIZE=$sizepfile
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
    qsub -V wordgenerator.pbs.sub 
    let ifile=ifile+lfile
  done
  let idx+=1
done
