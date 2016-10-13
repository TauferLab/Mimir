#!/usr/bin/python
import sys
import random
import string
import argparse

# seed for reproducibility
random.seed(0)

parser=argparse.ArgumentParser(description='Generate words.')
parser.add_argument("dist",help="distribution of word count")
parser.add_argument("nunique",type=int,help="number of unique words")
parser.add_argument("fsize",type=long,help="file size of each file")
parser.add_argument("offset",type=int,help="file index offset")
parser.add_argument("fcount",type=int,help="file count")
parser.add_argument("prefix",help="output file prefix, filename=prefix.i.txt")
parser.add_argument("outdir",help="output directory")
parser.add_argument("--config", nargs=1, default="wordgenerator.config", help="configuration file")
args = parser.parse_args()

print args

dist=args.dist
nunique=args.nunique
fsize=args.fsize
offset=args.offset
fcount=args.fcount
prefix=args.prefix
outdir=args.outdir
cfile=args.config

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

def gen_word_uniform(word_dict):
    return word_dict[random.randint(0, len(word_dict)-1)]

def gen_word_triangular(words):
    return word_dict[int(round(random.triangular(0, len(word_dict)-1, 0)))]

def gen_word_invalid(word_dict):
    print "invalid distribution"
    exit()

### SCRIPT ###


# read config file
conffile = open(cfile).readlines()
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
words_dist=dist 
gen_word = gen_word_invalid

if words_dist=="uniform":
    gen_word = gen_word_uniform
elif words_dist=='triangular':
    gen_word = gen_word_triangular


def gen_unique_words(nwords):
    all_words = {}
    print "begin to generate word bank with %d unique words\n" % nwords
    for i in xrange(0, nwords):
        wordlen = get_dist_value(conf, "length")
        word = ""
        while True:
            for j in xrange(0,wordlen):
                word+=alphabet[random.randint(0, len(alphabet)-1)]
            if not all_words.has_key(word):
                all_words[word] = 1
                break

    return all_words.keys()


def gen_line(length, word_dict):
    line = ""
    new_words = []
    length_left = length
    while length_left > 0:
        new_words.append(gen_word(word_dict))
        length_left -= len(new_words[-1])

    line = " ".join(new_words)

    if length_left < 0:
        return line[:length]

    return line


def main():
    word_dict = gen_unique_words(nunique)
# generate file
    print "output to %s" % outdir
    print "file count %d" % fcount

    for i in xrange(0,fcount):
        print "output file:"+prefix+'.'+str(offset+i)+'.txt'
        fid=open(outdir+'/'+prefix+'.'+str(offset+i)+'.txt','w')
        bytes_left = fsize
        curlen = 0
        curline = ""
        notstop = True
        new_lines = []
        while bytes_left > 0:
        # pick length of line
            chars_per_line = get_dist_value(conf, "chars_per_line")

            new_lines.append(gen_line(chars_per_line, word_dict))

            bytes_left -= len(new_lines[-1])

        lines = "\n".join(new_lines)

        fid.write(lines[:fsize])
        fid.close()

if __name__ == "__main__":
    main()

