#!/bin/bash
#----------------------------------------------------
# get the file list to meet the file size limitation
#
# Notes:
#
#   -- Launch this script by executing
#      "get_file_list.sh indir sumsize ext"
#
#----------------------------------------------------

indir=$1
sumsize=$2
ext=$3

filelist=$(find $indir -type f -name "*"$ext -exec ls -ld {} +                \
            | awk -v sumsize="$sumsize"                                        \
              'BEGIN { total = 0; count = 0;}
              {
                  if (total + $5 < sumsize) {
                      total += $5;
                      count += 1;
                      print $9
                  }
              };
              END {}')

echo $filelist
