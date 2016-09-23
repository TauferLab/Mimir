#!/usr/bin/python
import sys
import glob, os
import pandas as pd

from pandas import Series, DataFrame

if len(sys.argv)<4:
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

#curdir=os.path.dirname(os.path.realpath(__file__))

#col_str=['size','index','rank','map','comm','convert','reduce', 'rsize', 'wsize', 'rcount', 'wcount', 'rtime', 'wtime']
col_str=['testtime', 'hostname', 'memsize', 'peakmem', 'dataset', 'size', 'index', 'rank', 'tnum', 'tid', 'total', 'map', 'reduce', \
  'tidle', 'tbarrier', 'alltoall', 'wait', 'ialltoallv', 'alltoallv', 'allreduce', \
  'pfsopen', 'pfsseek', 'pfsread', 'pfsclose', 'memcpy', 'fop', 'atomic',\
  'sendbufsize', 'recvbufsize', 'mapinputsize', 'mapoutkv', \
  'cpsbucketsize', 'cpsuniquesize', 'cpskmvsize', 'cpsoutputkv', \
  'cvtbucketsize', 'cvtuniquesize', 'cvtsetsize', 'cvtkmvsize', \
  'rdcinputkv', 'rdcoutputkv', \
  'sendsize', 'recvsize', 'sendpadding', 'recvpadding', \
  'nfile', 'filesize', 'kvcount', 'nunique', 'a2acount']

event_mapper={'event_omp_idle':13,\
              'event_omp_barrier':14,\
              'event_comm_alltoall':15,\
              'event_comm_wait':16,\
              'event_comm_ialltoallv':17,\
              'event_comm_alltoallv':18,\
              'event_comm_allreduce':19,\
              'event_pfs_open':20,\
              'event_pfs_seek':21,\
              'event_pfs_read':22,\
              'event_pfs_close':23,\
              'event_mem_copy':24}

timer_mapper={'timer_map_fop':25,\
              'timer_map_atomic':26}

counter_mapper={'counter_comm_send_buf':27,\
                'counter_comm_recv_buf':28,\
                'counter_map_input_size':29,\
                'counter_map_output_kv':30,\
                'counter_cps_bucket_size':31,\
                'counter_cps_unique_size':32,\
                'counter_cps_kmv_size':33,\
                'counter_cps_output_kv':34,\
                'counter_cvt_bucket_size':35,\
                'counter_cvt_unique_size':36,\
                'counter_cvt_set_size':37,\
                'counter_cvt_kmv_size':38,\
                'counter_rdc_input_kv':39,\
                'counter_rdc_output_kv':40,\
                'counter_comm_send_size':41,\
                'counter_comm_recv_size':42,\
                'counter_comm_send_padding':43,\
                'counter_comm_recv_padding':44,\
                'counter_map_file_count':45,\
                'counter_map_file_size':46,\
                'counter_map_kv_count':47,\
                'counter_cvt_nunique':48}

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
    print dataset+"-"+str(node)
  #for dataset in datasets:
    #for node in nodelist:
    idx=0
    nproc=node*ppn
      #filefilter=prefix+"."+str(nproc)+"*.txt";
    filefilter=prefix.replace("*",dataset)+"."+str(nproc)+"*.txt"; 
    print filefilter
    for filename in glob.glob(indir+filefilter):
      print filename
      tmp=filename.split('_')
        #print tmp
      testtime=tmp[len(tmp)-1][:-4]
        #print testtime
        # open profile data file
      with open(filename) as f:
        lines = f.readlines()
          # handle one line
        i=0
        print len(lines)
        while i < len(lines):
          line=str(lines[i])
          #print line
          #line+=str(lines[i+1])
          #i+=1
          line=line.replace('\n','')
          elems=line.split(',') 
          size=0
          rank=0
          tnum=0
          profiler='disable'
          tracker='disable'
          hostname=''
          peakmem=''
          memsize=''
            # process meta-information of one process
          for j in range(len(elems)):
            elem=str(elems[j])
            token=elem.split(':')
            if token[0]=='rank':
              rank=int(token[1])
            elif token[0]=='size':
              size=int(token[1])
            elif token[0]=='thread':
              tnum=int(token[1])
            elif token[0]=='profiler':
              profiler=token[1]
            elif token[0]=='tracker':
              tracker=token[1]
            elif token[0]=='hostname':
              hostname=token[1]
            elif token[0]=='memory':
              memsize=int(token[1])
            elif token[0]=='peakmem':
              peakmem=int(token[1])
          #print hostname
          i+=1
          a2acount=0
            #print profiler
            #print tracker
            # process information of one process
          for j in range(tnum):
            item_data=[testtime, hostname, memsize, peakmem,\
                   dataset, size, idx, rank, tnum, 0, 0.0, 0.0, 0.0, \
                   0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, \
                   0.0, 0.0, 0.0, 0.0, 0.0,\
                   0, 0, 0, 0, 0, 0, 0, 0, 0, \
                   0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            if profiler=='enable':
              line=str(lines[i])
              line=line.replace('\n','')
              elems=line.split(',')
              for k in range(len(elems)):
                elem=str(elems[k])
                token=elem.split(':')
                if token[0] in timer_mapper:
                  item_data[timer_mapper[token[0]]]=float(token[1])
                if token[0] in counter_mapper:
                  item_data[counter_mapper[token[0]]]=int(token[1])  
              i+=1
            if tracker=='enable':
              line=str(lines[i])
              line=line.replace('\n','')
              elems=line.split(',')
              phases=-1
              for k in range(len(elems)):
                elem=str(elems[k])
                token=elem.split(':')
                if token[0] == 'action':
                  pass
                elif token[0] == 'event_mr_general':
                  phases+=1
                else:
                  item_data[10]+=float(token[1])
                  #item_data[9+phases]+=float(token[1])
                  pass
                if token[0] in event_mapper:
                  if token[0]=='event_comm_ialltoallv' or token[0]=='event_comm_alltoallv':
                    a2acount+=1
                  item_data[event_mapper[token[0]]]+=float(token[1])
              i+=1
            item_data[8]=j
            item_data[49]=a2acount
            #print item_data
            data.loc[len(data)]=item_data
      idx+=1
    itest+=1

  data=data.sort_values(by=['dataset', 'size', 'index', 'rank', 'tnum', 'tid'])
  data.to_csv(outfile, index=False)
  #print data

if __name__ == "__main__":
  main() 
