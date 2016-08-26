#!/bin/bash
python draw_key_value_dist.py  words_uniform_1G.txt words_uniform_dist --ylim 0 2000
python draw_key_value_dist.py  words_triangular_1G.txt words_triangular_dist --ylim 0 4000
python draw_key_value_dist.py  words_wikipedia_1G.txt words_wikipeida_dist --ylim -1000 100000

