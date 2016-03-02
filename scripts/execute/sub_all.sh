TIMES=10

PRE=$(qsub run_wc.1.sub)

START=1
END=0

for((k=$START; k<=$END; k*=2))
do
  for((i=1; i<=$TIMES; i++))
  do
    PRE=$(qsub -W depend=afterany:$PRE run_wc.$k.sub)
  done
done


