#!/usr/bin/python
import sys
import random
import string
import argparse

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

### SCRIPT ###

# seed for reproducibility
random.seed(0)

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
word_combs = dict()
num_words = nunique

print "begin to generate word bank with "+str(num_words)+" unique words\n"

#print xrange(num_words)

for i in range(0, num_words):
    wordlen = get_dist_value(conf, "length")
    word = ""
    while True:
      for j in range(0,wordlen):
        word+=alphabet[random.randint(0, len(alphabet)-1)]
      print word
      if word not in words:
        break
    print i
    #print word
    words.append(word)

print "generate word bank with "+str(num_words)+" unique words\n"

words_dist=dist

print "output to "+outdir+"\n"
print "file count "+str(fcount)+"\n"

# generate file
for i in range(0,fcount):
  print "output file:"+prefix+'.'+str(offset+i)+'.txt\n'
  fid=open(outdir+'/'+prefix+'.'+str(offset+i)+'.txt','w')
  bytes_left = fsize
  curlen = 0
  curline = ""
  notstop = True
  while notstop:
    # pick length of line
    chars_per_line = get_dist_value(conf, "chars_per_line")

    print chars_per_line

    # fill line with words 
    while curlen < chars_per_line:
        w = ""
        if words_dist=="uniform":
          w = words[random.randint(0, len(words)-1)] + " "
        elif words_dist=='triangular':
          w = words[int(round(random.triangular(0, len(words)-1, 0)))] + " "
        else:
          print "distribution error:"+dist
          exit(1)
        #print w
        wlen = len(w)
        if curlen + wlen >= bytes_left:
            curline += w
            curlen += wlen
            if curlen > bytes_left:
                curline = curline[:(bytes_left-curlen)]
            fid.write(curline)
            #print curline
            #sys.stdout.write(curline)
            notstop=False
            break
        elif curlen + wlen >= chars_per_line:
            bytes_left -= (curlen + wlen)
            w = w[:-1] + "\n"
            fid.write(curline+w)
            #print curline
            #sys.stdout.write(curline + w)
            curlen = 0
            curline = ""
            break
        else:
            curlen += wlen
            curline += w
  fid.close()
