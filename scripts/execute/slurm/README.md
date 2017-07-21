# Workflow
* ./make_all.sh /mimir/install/dir
* Copy config.example.h and test.example.sh to customize the tests
* ./clean_all.sh

# Scripts
* make_all.sh --- make && install mimir
* clea_all.sh --- cleanup logs
* slurm.ibrun.sub --- submit script (ibrun)
* slurm.mpiexec.sub --- submit script (mpiexec)
* run.job.sh --- wrapper to submit a job
* config.example.h --- configuration file for a test
* test.example.sh --- submit script to submit tests
* get_file_list.sh --- get the file list to match the file size
