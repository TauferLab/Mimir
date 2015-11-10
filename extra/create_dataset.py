import os
import sys
import random
import string

filedir=''

minword = 0
maxword = 10000

def get_file_size(filename):
    fullname = os.path.join(filedir, filename)
    filesize = os.path.getsize(fullname)
    return filesize

def create_data_file(filename, maxsize):
    fullname = os.path.join(filedir, filename)
    f = file(fullname, "w")
    count = 0
    while True:
       num = random.randrange(minword, maxword)
       word = "%i"%num
       word += ' '
       f.write(word)
       count += 1
       if count % 10 == 0:
           f.write('\n')
       filesize = get_file_size(filename)
       filesize /= (1024)
       if filesize >= maxsize:
           break
    f.close()

def create_multi_files(count, maxsize):
    for i in range(0, count):
        filename = "%i"%i
        filename += "."
        filename += "%i"%maxsize
        filename += "M"
        create_data_file(filename, maxsize)

# main function
count = string.atoi(sys.argv[1])
maxsize = string.atoi(sys.argv[2])
if len(sys.argv) > 3:
    filedir = sys.argv[3]
    if os.path.exists(filedir) == False:
        os.mkdir(filedir)
create_multi_files(count, maxsize)
