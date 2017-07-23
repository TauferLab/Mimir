"""
Filename format:
    prefix-_date_profile.txt
"""
#!/bin/python
import sys, os, glob
import pandas as pd
from figlib import *

from pandas import Series, DataFrame

print(sys.argv)

parser=argparse.ArgumentParser(
        description='Draw comparison of metrics')

"data configuration"
parser.add_argument('filefilter', help='file filter string')
parser.add_argument('var1list', help='first variable list, represent by #1')
parser.add_argument('var2list', help='second variable list, represent by #2')

"directory configuration"
parser.add_argument('--indir', default='./', help='input directory')
parser.add_argument('--outdir', default='./', help='output directory')
parser.add_argument('--outfile', default='test.pdf', help='output file')

"figure configuration"
parser.add_argument('--xticklists', help='tick list of x axis')
parser.add_argument('--xlabelname', default='settings', help='label name of x axis')
parser.add_argument('--legendlist', help='legend list')
parser.add_argument('--colorlist', default='darkviolet,red,blue,green', help='color list of line')
parser.add_argument('--markerlist', default='*,^,v,o', help='marker list')
parser.add_argument('--ylim', nargs=2, type=float, help='y range of line bar')
parser.add_argument('--ylog', type=int, help='ylog')
args = parser.parse_args()

def get_one_test_results(filefilter, indir):

    datalist=[]
    print indir
    print filefilter
    for filename in glob.glob(indir+'/'+filefilter):
        print 'read file:'+filename
        file_data=pd.read_csv(filename)
        file_data=file_data.groupby(by='testtime').mean()
        file_data['filefilter']=[filefilter]*len(file_data.index)
        datalist.append(file_data)

    if datalist:
        datalist=pd.concat(datalist)
    else:
        datalist = None;

    return datalist

def get_one_group_results(filefilter, var1, var2, indir):
    fig_data = []
    for v1 in var1:
        for v2 in var2:
            filename=filefilter.replace('#1',v1, 1).replace('#2',v2, 1)
            item_data = get_one_test_results(filename, indir)
            if item_data is not None:
                item_data['xlabel'] = [v1] * len(item_data.index)
                item_data['huelabel'] = [v2] * len(item_data.index)
                fig_data.append(item_data)
    fig_data=pd.concat(fig_data)
    print fig_data
    return fig_data

def main():
    fig_data = get_one_group_results(args.filefilter,
            args.var1list.split(','),
            args.var2list.split(','), args.indir)

    xticklists = []
    legendlist = []

    if args.xticklists is not None:
        xticklists = args.xticklists.split(',')
    if args.legendlist is not None:
        legendlist = args.legendlist.split(','),

    draw_line(fig_data, args.outdir + args.outfile,
            order = args.var1list.split(','),
            hue_order = args.var2list.split(','),
            xticklist = xticklists,
            legendlist = legendlist,
            colorlist = args.colorlist.split(','), 
            markerlist = args.markerlist.split(','), 
            xlabelname=args.xlabelname, 
            ylim = args.ylim, ylog = args.ylog)

main()

#def test():
    #get_one_test_results('mimir-d32M-c32M-p32M-bfs-graph500-s25-normal-1-68',
    #        '../stampede2-singlenode')
    #get_one_group_results('mimir-d32M-c32M-p32M-bfs-graph500-s25-#1-1-#2',
    #        ['normal', 'flat-snc4'], ['68', '136'], '../stampede2-singlenode')

#test();
