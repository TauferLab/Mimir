#!/bin/bash

python draw_mimir_total_time.py wordcount wikipedia \
    weakscale4G 8G,16G,32G,64G,128G,256G \
    --settings "basic,kvhint" \
    --indir ../../data/comet/wc_wikipedia_weakscale4G_c64M-p64M-i512M-h20/
