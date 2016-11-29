#!/bin/python
import argparse

from cleanup_data import *
from draw_figures import * 

parser=argparse.ArgumentParser(\
  description='Draw total execution time figure')
parser.add_argument('benchmark', default='wordcount', help='benchmark name')
parser.add_argument('datatype', default='wikipedia', help='data type')
parser.add_argument('testtype', default='weakscale4G', help='test type')
parser.add_argument('datalist', default='', help='data list')
parser.add_argument('xticklist', default='', help='data list')
parser.add_argument('--config', default='c64M-p64M-i512M-h20', help='library configuration')
parser.add_argument('--settings', default='basic,kvhint,cbkvhint', help='library settings')
parser.add_argument('--indir', default='../../data/comet', help='input directory')
parser.add_argument('--outdir', default='../../figures', help='output directory')
parser.add_argument('--outfile', default='test', help='output file')
parser.add_argument('--labellist', default='Mimir,Mimir(hint),Mimir(kvhint;cb)')
parser.add_argument('--colorlist', default='')
parser.add_argument('--markerlist', default='')
parser.add_argument('--xlabelname', default='')
parser.add_argument('--ylim', metavar='int', type=float, nargs=2, \
  default=[0.0,5.3], help='range in y axis')


args = parser.parse_args()


def main():
    settings=args.settings.split(',')
    datalist=args.datalist.split(',')
    fig_data=get_results_of_one_benchmark(\
        library="mimir", \
        config=args.config, \
        settings=settings, \
        benchmark=args.benchmark, \
        datatype=args.datatype, \
        testtype=args.testtype, \
        datalist=datalist, \
        indir=args.indir)

    labellist=args.labellist.split(',')
    colorlist=args.colorlist.split(',')
    markerlist=args.markerlist.split(',')
    xticklist=args.xticklist.split(',')
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
        ylim=args.ylim)

if __name__ == "__main__":
    main()
