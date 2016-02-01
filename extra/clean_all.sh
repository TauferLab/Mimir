start_index=1
end_index=64
prefix=20G

for((i=$start_index;i<$end_index;i++))
do
  cd $prefix.$i && rm *.py *.sh *.sub* one* rr.keys* rr.points* && cd ..
done
