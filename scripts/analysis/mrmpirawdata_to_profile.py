#!/usr/bin/python
import sys
import glob, os
import pandas as pd

from pandas import Series, DataFrame

if len(sys.argv)<6:
  print sys.argv
  print "usage: [prefix] [dataset] [ppn] [nodelist: 1,2,...] [indir] [outdir]"
  sys.exit(1)

prefix=sys.argv[1]
datasets=sys.argv[2].split(',')
ppn=int(sys.argv[3])
nodelist=sys.argv[4].split(',')
for i in range(0,len(nodelist)):
  nodelist[i]=int(nodelist[i])
indir=sys.argv[5]
outdir=sys.argv[6]

testtype='onenode'

if prefix.find('onende') != -1:
  testtype='onenode'
elif prefix.find('weekscale') != -1:
  testtype='weekscale'

col_str=['testtime','dataset','size','index','rank',\
  'total','general','map','comm','convert','reduce',\
  'pfstime','rtime','wtime','MPI_Alltoall','MPI_Alltoallv',\
  'MPI_Allreduce','MPI_Reducescatter','rsize','wsize',\
  'npagemax','sendsize','recvsize','a2acount', 'rcount','wcount']

int_mapper={
  "nprocs":2,
  "me":4
};

float_mapper={
  "readsize":18,
  "writesize":19,
  "npagemax":20,
  "send_size":21,
  "recv_size":22
};

event_mapper={
  "mr_general":6,
  "mr_map":7,
  "mr_aggregate":8,
  "mr_convert":9,
  "mr_reduce":10,
  "mr_find_file":7,
  "mr_bcast_file":7,
  "mr_comm_alltoall":14,
  "mr_comm_alltoallv":15,
  "mr_comm_allreduce":16,
  "mr_comm_reducescatter":17,
  "mr_file_stat":11,
  "mr_file_open":11,
  "mr_file_read":11,
  "mr_file_write":11,
  "mr_file_close":11,
  "mr_kv_page_open":12,
  "mr_kv_page_close":12,
  "mr_kv_page_seek":12,
  "mr_kv_page_read":12,
  "mr_kv_page_write":13,
  "mr_kmv_page_open":12,
  "mr_kmv_page_close":12,
  "mr_kmv_page_seek":12,
  "mr_kmv_page_read":12,
  "mr_kmv_page_write":13,
  "mr_spool_page_open":12,
  "mr_spool_page_close":12,
  "mr_spool_page_read":12,
  "mr_spool_page_write":13
};

#zero_item=[0,0,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0,0,0,0]

def main():
  to_phases_data(outdir+prefix.replace("-*","").replace("*", "")+'.ppn'+str(ppn)+'_phases.txt')

def to_phases_data(outfile):
  #os.chdir(indir)
  data=DataFrame(columns=col_str)
  ntests=0
  if len(datasets)==len(nodelist):
    ntests=len(datasets)
  elif len(nodelist)==1:
    ntests=len(datasets)
  elif len(datasets)==1:
    ntests=len(nodelists)
  else:
    print "the parameters of datset or nodelist is error!\n"
    exit()

  dataset=""
  node=0
  ntests=0
  if testtype=='onenode' or testtype=='weekscale':
    ntests=len(datasets)
  itest=0
  while itest < ntests:
    if testtype=='onenode':
      dataset=datasets[itest]
      node=1
    elif testtype=='weekscale':
      dataset=datasets[itest]
      node=nodelist[itest]
  #for dataset,node in datasets,nodelist:
    print dataset+"-"+str(node)
    #for node in nodelist:
    idx=0
    nproc=node*ppn
    filefilter=prefix.replace("*",dataset)+"."+str(nproc)+"*.txt";
    print filefilter
    for filename in glob.glob(indir+filefilter):
      print filename
      tmp=filename.split('_')
      #print tmp
      testtime=tmp[len(tmp)-1][:-4]
      print testtime
      # open profile data file
      with open(filename) as f:
        lines = f.readlines()
        # handle one line
        for i in range(len(lines)):
          item_data=[testtime,dataset,0,idx,0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,\
                     0.0,0.0,0.0,0.0,0.0,0.0,0,0,0,0,0,0,0,0]
          phase=-1
          phase_flag=0
          timer_flag=0
          line=str(lines[i])
          line=line.replace('\n','')
            #ptime=[]
          a2acount=0
          elems=line.split(',')
            # handle one element
          for j in range(len(elems)):
            elem=str(elems[j])
            token=elem.split(':')
              #print elem
              # handle token
            if token[0] in int_mapper:
              item_data[int_mapper[token[0]]]=int(token[1])
            elif token[0] in float_mapper:
              item_data[float_mapper[token[0]]]=float(token[1])
 
            elif token[0]=='action':
              if token[1]=='timer_start':
                timer_flag=1
              elif token[1]=='timer_stop':
                timer_flag=0
            else: 
              if timer_flag==1:
                if token[0] in event_mapper:
                  if token[0]=='mr_comm_alltoallv':
                    a2acount+=1
                  item_data[event_mapper[token[0]]]+=float(token[1])
                else:
                  print token[0]
                if phase_flag==1 and token[0]=='mr_general':
                  phase_flag=0
                elif phase_flag==0 and token[0]!='mr_general':
                  phase_flag=1
                  #ptime.append(float(token[1]))
                  phase+=1
                    #print elem
                    #print phase
                if phase_flag==1:
                  #item_data[item_off+4+phase]+=float(token[1])
                  item_data[5]+=float(token[1])
                  #ptime[phase]+=float(token[1])
            #item_data[5]=ptime
          item_data[23]=a2acount
          data.loc[len(data)]=item_data
      idx+=1
    itest+=1

  data=data.sort_values(by=['dataset', 'size', 'index', 'rank'])
  data.to_csv(outfile, index=False)
  #print data

if __name__ == "__main__":
  main() 
