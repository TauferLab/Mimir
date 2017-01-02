#!/bin/python
from datafilter import *
from figdrawer import * 

#config="c64M-p64M-i512M-h20"

parser=argparse.ArgumentParser(description='Draw peak memory usage and \
        execution time of four figures')

"library and running configuration"
parser.add_argument('--testtype', default='singlenode', help='test type')
parser.add_argument('--benchmarks', nargs=4, 
        default=['wordcount','wordcount','octree','bfs'], 
        help='benchmark list')
parser.add_argument('--datatypes', nargs=4, 
        default=['uniform','wikipedia','1S','graph500'], 
        help='data type list')
parser.add_argument('--datalists', nargs=4, 
        default=['256M,512M,1G,2G,4G,8G,16G',
            '256M,512M,1G,2G,4G,8G,16G',
            'p24,p25,p26,p27,p28,p29,p30',
            's19,s20,s21,s22,s23,s24,s25,s26'],
        help='data list of each running')
parser.add_argument('--config', default='c64M-p64M-i512M-h20', 
        help='library configuration')
parser.add_argument('--mrmpiversion', default='p64,p512', 
        help='version of mrmpi')
parser.add_argument('--mimirversion', default='basic', help='version of mimir')
parser.add_argument('--upperlists', nargs=4, default=['32G,1G,8G','32G,1G,8G',\
        'p31,p26,p29', 's27,s21,s24'], \
        help='upper list to cut off extra results')

"directory configuration"
parser.add_argument('--basedir', default='../../../gclab/projects/mimir/data/', \
        help='base directory')
parser.add_argument('--mrmpidirs', nargs=4, \
        default=['mrmpi/comet','mrmpi/comet','mrmpi/comet','mrmpi/comet'], \
        help='mrmpi input data directory list')
parser.add_argument('--mimirdirs', nargs=4, \
        default=['comet/wc_uniform_singlenode_c64M-p64M-i512M-h20',\
        'comet/wc_wikipedia_singlenode_c64M-p64M-i512M-h20',\
        'comet/octree_1S_singlenode_c64M-p64M-i512M-h20',\
        'comet/bfs_graph500_singlenode_c64M-p64M-i512M-h20'], \
        help='mimir input directory list')
parser.add_argument('--outdir', default='../../figures/', 
        help='output directory')
parser.add_argument('--outfile', default='test.pdf', help='output file')

"figure configuration"
parser.add_argument('--xticklists', nargs=4, 
        default=['256M,512M,1G,2G,4G,8G,16G',
            '256M,512M,1G,2G,4G,8G,16G',
            '2^24,2^25,2^26,2^27,2^28,2^29,2^30',
            '2^19,2^20,2^21,2^22,2^23,2^24,2^25,2^26'], 
        help='xtick list of each running')
parser.add_argument('--labellist', default='Mimir,MR-MPI (64M),MR-MPI (512M)',\
        help='label list of different configuration')
parser.add_argument('--xlabelname', nargs=4, \
        default=['dataset size','dataset size',\
        'number of points','number of vertices'],\
        help='the label name of x axes')
parser.add_argument('--barcolors', 
        default='coral,yellow,lightblue,lightgreen',
        help='color list of bar')
parser.add_argument('--linecolors', \
        default='darkviolet,red,blue,green', \
        help='color list of line')
parser.add_argument('--hatches', default='x+/o',\
        help='hatches of the bar')
parser.add_argument('--markerlist', default='*,^,v,o',\
        help='markers of the line')
parser.add_argument('--barlims', nargs=4, \
        default=['0,5','0,5','0,5','0,5'], \
        help='y range of line bar')
parser.add_argument('--linelims', nargs=8, metavar='int', type=int, \
        default=[-20,80,-20,80,-40,150,-40,100],\
        help='y range of the line')
parser.add_argument('--subtitles', nargs=4, \
        default=['WC(Uniform)','WC(Wikipedia)','OC','BFS'],\
        help='the titles of subfigures')


#parser.add_argument('datatype', default='wikipedia', help='data type')
#parser.add_argument('testtype', default='weakscale4G', help='test type')
#parser.add_argument('datalist', default='', help='data list')
#parser.add_argument('--config', default='c64M-p64M-i512M-h20', help='library configuration')
#parser.add_argument('--indir1', default='../../data/comet', help='input directory')
#parser.add_argument('--indir2', default='../../data/comet', help='input directory')
#parser.add_argument('--outdir', default='../../figures', help='output directory')
#parser.add_argument('--outfile', default='test', help='output file')
#parser.add_argument('--labellist1', default='MR-MPI,MR-MPI (64M)')
#parser.add_argument('--labellist2', default='Mimir')
#parser.add_argument('--markerlist', default='')
#parser.add_argument('--hatches', default='')
#parser.add_argument('--memlim', metavar='int', type=float, nargs=2, \
#  default=[0.0,5.3], help='range in y axis')
#parser.add_argument('--timelim', metavar='int', type=float, nargs=2, \
#  default=[0.0,5.3], help='range in y axis')
args = parser.parse_args()

    #datalist=args.datalist.split(',')
    #upperlist=args.upperlist.split(',')
    #xticklist=args.xticklist.split(',')



def main():

    mimirversion=args.mimirversion.split(',')
    if args.mrmpiversion!='':
        mrmpiversion=args.mrmpiversion.split(',')
        settings=mimirversion+mrmpiversion
    else:
        settings=mimirversion

    "get fig data list"
    fig_data_list=[]
    
    for i in range(0,4):
        datalist=args.datalists[i].split(',')

        fig_data=[]
        
        "get Mimir data"
        mimir_data=get_results_of_one_benchmark(\
                library="mimir", \
                config=args.config, \
                settings=mimirversion, \
                benchmark=args.benchmarks[i], \
                datatype=args.datatypes[i], \
                testtype=args.testtype, \
                datalist=datalist, \
                indir=args.basedir+args.mimirdirs[i])  
        mimir_data=mimir_data[["dataset", "setting", \
                "total_time", "peakmem_use"]]
        fig_data.append(mimir_data)

        "get MR-MPI data"
        if args.mrmpiversion!='':
            mrmpi_data=get_mrmpi_results_of_one_benchmark(\
                    library='mrmpi',\
                    config='a2a', \
                    settings=mrmpiversion, \
                    benchmark=args.benchmarks[i], \
                    datatype=args.datatypes[i], \
                    testtype=args.testtype, \
                    datalist=datalist, \
                    indir=args.basedir+args.mrmpidirs[i])
            mrmpi_data=mrmpi_data[["dataset", "setting", \
                    "total_time", "peakmem_use"]]
            fig_data.append(mrmpi_data)
       
        "merge data from MR-MPI and Mimir"
        fig_data=pd.concat(fig_data)
       
        "filter out the data exceed the upper bound"
        filtered_data=[]
        upperlist=args.upperlists[i].split(',')

        print upperlist
        print settings
        idx=0
        for setting in settings:
            for dataset in datalist:
                if upperlist[idx]==dataset:
                    break;
                filtered_data.append(fig_data[(fig_data['setting']==setting) \
                        & (fig_data['dataset']==dataset)])
            idx+=1
        fig_data=pd.concat(filtered_data)

        fig_data_list.append(fig_data)

    #labellist1=args.labellist1.split(',')
    #labellist2=args.labellist2.split(',')
    #labellist=labellist1+labellist2
    #settings1=args.settings1.split(',')
    #settings2=args.settings2.split(',')
    #settings=settings1+settings2

    #memcolor=args.memcolor.split(',')
    #timecolor=args.timecolor.split(',')
    #markerlist=args.markerlist.split(',')

    #print fig_data_list
    #fig_data['dataset']=fig_data['dataset'].apply(str) 
    #print fig_data

    xticklists=[]
    for i in range(0,4):
        xticklists.append(args.xticklists[i].split(','))

    barlims=[]
    linelims=[]
    for i in range(0,4):
        barlim=args.barlims[i].split(',')
        linelim=[args.linelims[2*i],args.linelims[2*i+1]]
        barlims.append(barlim)
        linelims.append(linelim)

    for i in range(0,4):
        barlims[i][0]=int(barlims[i][0])
        barlims[i][1]=int(barlims[i][1])
        linelims[i][0]=int(linelims[i][0])
        linelims[i][1]=int(linelims[i][1])

    print barlims
    print linelims
    fig_data=draw_subfigures_memory_and_time(\
        datalist=fig_data_list, \
        settings=settings, \
        outdir=args.outdir, \
        outfile=args.outfile,\
        subtitles=args.subtitles,\
        xticklists=xticklists, \
        xlabelname=args.xlabelname, \
        labellist=args.labellist.split(','),\
        barcolors=args.barcolors.split(','), \
        linecolors=args.linecolors.split(','), \
        hatches=args.hatches,\
        markerlist=args.markerlist.split(','),\
        barlims=barlims,\
        linelims=linelims)
  
if __name__ == "__main__":
    main()
