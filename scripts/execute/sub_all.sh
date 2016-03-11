TIMES=10
START=1
END=32

PRE=$(qsub run_wc.1.sub)

for((i=1; i<=$TIMES; i++))
do
  for((k=$START; k<=$END; k*=2))
  do
    PRE=$(qsub -W depend=afterany:$PRE run_wc.$k.sub)
  done
done
