#!/bin/python
from cleanup_data import *
from draw_figures import * 

config="c64M-p64M-i512M"



def main():
    fig_data=get_results_of_one_benchmark(\
        library="mimir", \
        config="c64M-p64M-i512M", \
        settings=["basic", "cb", "kvhint", "cbkvhint"], \
        benchmark="bfs", \
        datatype="graph500", \
        testtype="weakscale20", \
        datalist=["s21", "s22", "s23", "s24", "s25", "s26"], \
        indir="../../data/comet/bfs_weakscale20")
    
    fig_data=draw_total_time(\
        data=fig_data, \
        outdir="../../figures", \
        outfile="test.pdf", \
        xticklist=["2^21", "2^22", "2^23", "2^24", "2^25", "2^26"], \
        labellist=["Mimir", "Mimir(cb)", "Mimir(kvhint)", "Mimir(cb;kvhint)"],\
        colorlist=["coral", "yellow", "lightblue", "red"], \
        markerlist=["*","v","o","1"])

if __name__ == "__main__":
    main()
