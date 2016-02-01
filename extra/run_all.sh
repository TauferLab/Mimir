start_index=0
end_index=64
prefix=20G

for((i=$start_index;i<$end_index;i++))
do
  mkdir $prefix.$i && cp ./script/* ./$prefix.$i && cd $prefix.$i && qsub -l nodes=1:ppn=1,walltime=04:00:00 ./gen_syn_data.sub && cd ..
done
