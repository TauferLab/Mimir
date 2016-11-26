#!/bin/python
from cleanup_data import *
from draw_memory_and_time import * 

def main():
    fig_data=get_results_of_one_benchmark("mimir", "c64M-p64M-i512M", ["basic", "cb"], \
        "bfs", "graph500s16", "singlenode", ["s20", "s21", "s22", "s23", "s24", "s25", "s26", "s27"], \
        "../../data/comet/bfs_singlenode", 24)
    
    fig_data=draw_memory_and_time(fig_data, "../../figures", "test.pdf", \
        xticklist=["2^20", "2^21", "2^22", "2^23", "2^24", "2^25", "2^26", "2^27"], \
        labellist=["Mimir", "Mimir(combine)"],\
        memcolors=["coral", "yellow", "lightblue"], \
        timecolors=["coral", "lightblue", "lightgreen"], \
        markerlist=["*","v","o"])

if __name__ == "__main__":
    main()
