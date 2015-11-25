
ligand_file=$1
start_id=$2
end_id=$3
path=$4

ligand=`more $ligand_file`
#path=/home/bzhang/mrmpi-7Apr14/benchmark_lg/data_config/2s_rr/
file_id=0
for (( i=$start_id;i<$end_id;i++ )); do
	id=0
	echo $id
	key_file=$path/rrkeys"$i".txt
	data_file=$path/rrdata"$i".txt
	exec<$key_file
	while read line; do
		data_line=$id" "$ligand" "$line
		echo $data_line >> $data_file
		let "id=id+1"
	done
done
