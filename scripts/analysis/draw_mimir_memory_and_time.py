#!/bin/python
from cleanup_data import *
from draw_figures import * 

config="c64M-p64M-i512M-h20"

parser=argparse.ArgumentParser(\
  description='Draw total execution time figure')
parser.add_argument('benchmark', default='wordcount', help='benchmark name')
parser.add_argument('datatype', default='wikipedia', help='data type')
parser.add_argument('testtype', default='weakscale4G', help='test type')
parser.add_argument('datalist', default='', help='data list')
parser.add_argument('--config', default='c64M-p64M-i512M-h20', help='library configuration')
parser.add_argument('--settings', default='basic,kvhint,cbkvhint', help='library settings')
parser.add_argument('--indir', default='../../data/comet', help='input directory')
parser.add_argument('--outdir', default='../../figures', help='output directory')
parser.add_argument('--outfile', default='test', help='output file')
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

    fig_data=draw_memory_and_time(\
        data=fig_data, \
        outdir=args.outdir, \
        outfile=args.outfile, \
        xticklist=datalist, \
        labellist=["Mimir", "Mimir(cb)", "Mimir(kvhint)", "Mimir(cb;kvhint)"],\
        memcolors=["coral", "yellow", "lightblue", "red"], \
        timecolors=["coral", "yellow", "lightblue", "red"], \
        markerlist=["*","v","o","1"])
  
if __name__ == "__main__":
    main()
