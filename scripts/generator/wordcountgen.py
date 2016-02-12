#!/usr/bin/python

import sys
import random
import string
import itertools

### FUNCTIONS ###

def get_dist_value(conf, var):
    res = -1
    if conf[var + "_dist"] == "uniform":
        varmin = int(conf[var + "_mean"]) - 3*int(conf[var + "_sd"])
        varmax = int(conf[var + "_mean"]) + 3*int(conf[var + "_sd"])
        res = int(round(random.uniform(varmin, varmax)))
    elif conf[var + "_dist"] == "normal":
        mu = int(conf[var + "_mean"])
        sigma = int(conf[var + "_sd"])
        res = int(round(random.gauss(mu, sigma)))
    elif conf[var + "_dist"] == "power":
        print "Error: has not been implemented"
        exit(1)
    elif conf[var + "_dist"] == "beta":
        alpha = float(conf[var + "_alpha"])
        beta = float(conf[var + "_beta"])
        varmin = int(conf[var + "_betamin"])
        varmax = int(conf[var + "_betamax"])
        res = varmin + int(round((varmax - varmin) * random.betavariate(alpha, beta)))

    if res < 0:
        print "Error: generating value from conf file"
        exit(1)
    
    return res

### SCRIPT ###

usage = "./wordcountgen.py <config-file>"

if len(sys.argv) < 2:
    print "Usage:\n\t" + usage
    exit(1)

# seed for reproducibility
random.seed(0)

# read config file
conffile = open(sys.argv[1]).readlines()
confarr = []
conf = None

for line in conffile:
    opt = line.strip("\n").split(":")
    if opt[0] == "":
        continue
    confarr.append(opt)

conf = dict(confarr)

# array of possible characters
alphabet = string.letters + string.digits

# generate bank of words
words = []
word_combs = dict()
num_words = int(conf["num_words"])
for i in xrange(num_words):
    wordlen = get_dist_value(conf, "length")

    if wordlen not in word_combs:
        word_combs[wordlen] = itertools.combinations_with_replacement(alphabet, wordlen)

    words.append(word_combs[wordlen].next())

# convert char tuples to strings
for i in xrange(len(words)):
    s = ""
    for j in xrange(len(words[i])):
        s += words[i][j]
    words[i] = s

# generate file
bytes_left = int(conf["num_lines"])*int(conf["chars_per_line_mean"])
curlen = 0
curline = ""
while True:
    # pick length of line
    chars_per_line = get_dist_value(conf, "chars_per_line")

    # fill line with words
    while curlen < chars_per_line:
        w = words[random.randint(0, len(words)-1)] + " "
        wlen = len(w)
        if curlen + wlen >= bytes_left:
            curline += w
            curlen += wlen
            if curlen > bytes_left:
                curline = curline[:(bytes_left-curlen)]
            sys.stdout.write(curline)
            exit(0)
        elif curlen + wlen >= chars_per_line:
            bytes_left -= (curlen + wlen)
            w = w[:-1] + "\n"
            sys.stdout.write(curline + w)
            curlen = 0
            curline = ""
            break
        else:
            curlen += wlen
            curline += w
