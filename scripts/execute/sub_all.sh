TIMES=10
START=128
END=1

PRE=$(qsub run_wc.1.sub)

for((k=$START; k<=$END; k*=2))
do
  for((i=1; i<=$TIMES; i++))
  do
    PRE=$(qsub -W depend=afterany:$PRE run_wc.$k.sub)
  done
done
