#!/bin/python
from datafilter import *
from figdrawer import * 

config="c64M-p64M-i512M-h20"

parser=argparse.ArgumentParser(description='Draw total execution time figure')
parser.add_argument('benchmark', default='wordcount', help='benchmark name')
parser.add_argument('datatype', default='wikipedia', help='data type')
parser.add_argument('testtype', default='weakscale4G', help='test type')
parser.add_argument('datalist', default='', help='data list')
parser.add_argument('upperlist', default='', help='upper list')
parser.add_argument('xticklist', default='', help='data list')
parser.add_argument('--config', default='c64M-p64M-i512M-h20', help='library configuration')
parser.add_argument('--settings1', default='p64,p512', help='library settings')
parser.add_argument('--settings2', default='basic', help='library settings')
parser.add_argument('--indir1', default='../../data/comet', help='input directory')
parser.add_argument('--indir2', default='../../data/comet', help='input directory')
parser.add_argument('--outdir', default='../../figures', help='output directory')
parser.add_argument('--outfile', default='test', help='output file')
parser.add_argument('--labellist1', default='MR-MPI,MR-MPI (64M)')
parser.add_argument('--labellist2', default='Mimir')
parser.add_argument('--colorlist', default='')
parser.add_argument('--markerlist', default='')
parser.add_argument('--hatches', default='')
parser.add_argument('--xlabelname', default='')
parser.add_argument('--ylim', metavar='int', type=float, nargs=2, \
  default=[0.0,5.3], help='range in y axis')
args = parser.parse_args()

def main():

    settings1=args.settings1.split(',')
    settings2=args.settings2.split(',')
    datalist=args.datalist.split(',')
    upperlist=args.upperlist.split(',')
    xticklist=args.xticklist.split(',')

    fig_data=[]

    # get Mimir data
    mimir_data=get_results_of_one_benchmark(\
        library="mimir", \
        config=args.config, \
        settings=settings1, \
        benchmark=args.benchmark, \
        datatype=args.datatype, \
        testtype=args.testtype, \
        datalist=datalist, \
        indir=args.indir1)  
    mimir_data=mimir_data[["dataset", "setting", \
        "total_time", "peakmem_use"]]

    # get MR-MPI data
    mrmpi_data=get_mrmpi_results_of_one_benchmark(\
        'mrmpi',\
        'a2a', settings2, args.benchmark, \
        args.datatype, args.testtype, datalist, \
        args.indir2)
    mrmpi_data=mrmpi_data[["dataset", "setting", \
        "total_time", "peakmem_use"]]
    fig_data.append(mrmpi_data)

    print mrmpi_data

    # merge MR-MPI and Mimir data
    fig_data.append(mimir_data)
    fig_data=pd.concat(fig_data)
    
    labellist1=args.labellist1.split(',')
    labellist2=args.labellist2.split(',')
    labellist=labellist1+labellist2

    settings1=args.settings1.split(',')
    settings2=args.settings2.split(',')
    settings=settings1+settings2

    print fig_data

    filtered_data=[]
    idx=0
    for setting in settings:
        for dataset in datalist:
            #print idx
            print dataset
            print upperlist[idx]
            if upperlist[idx]==dataset:
                break; 
            filtered_data.append(fig_data[(fig_data['setting']==setting) & (fig_data['dataset']==dataset)])
        idx+=1

    fig_data=pd.concat(filtered_data)
    
    print fig_data

    colorlist=args.colorlist.split(',')
    markerlist=args.markerlist.split(',')

    #print fig_data

    print colorlist
    fig_data=draw_total_time(\
        data=fig_data, \
        outdir=args.outdir, \
        outfile=args.outfile, \
        settings=settings, \
        xlabelname=args.xlabelname, \
        xticklist=xticklist, \
        labellist=labellist,\
        colorlist=colorlist, \
        markerlist=markerlist, \
        datalist=datalist, \
        ylim=args.ylim)
  
if __name__ == "__main__":
    main()
