#/bin/bash
#settings=( 'words_small_512M' 'wikipedia_small_512M' 'words_large_64M' 'wikipedia_large_64M')
#settings=('words_small_512M')
settings=('words_large_512M')
#settings=('words_strong_scalable')
#settings=( 'words_small_512M' 'wikipedia_small_512M')

TIMES=10
START=4
END=16

PREFIX="Submitted batch job "

PRE=$1

for((k=$START; k<=$END; k*=2))
do
  for setting in ${settings[@]}
  do
    echo $setting
    cd $setting
    for((i=1; i<=$TIMES; i++))
    do
      if [ $PRE == "none" ]
      then
        PRE=$(sbatch run.$k.sub)
        PRE=${PRE#$PREFIX}        
      else
        PRE=$(sbatch --dependency=afterany:$PRE run.$k.sub)
        PRE=${PRE#$PREFIX}
      fi
      echo $PRE
    done
    cd ..
  done
done
