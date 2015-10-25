CC = mpicc
CPP = mpic++
CLINK = mpicc
CPPLINK = mpic++
DEBUG = -g
CFLAGS = -fopenmp -std=c++11  $(DEBUG) -O2  -DDEBUG=0 -DFUSION
LFLAGS = 
ARCHIEVE = ar
AFLAGS =  -rcv
OBJS = mapreduce.o  hash.o kmv.o dataobject.o keyvalue.o keymultivalue.o

all: libmrmpi

libmrmpi: $(OBJS)
	$(ARCHIEVE) $(AFLAGS) libmrmpi.a $(OBJS)

mapreduce.o: mapreduce.cpp mapreduce.h
	$(CPP) $(CFLAGS) -c mapreduce.cpp

hash.o: hash.cpp hash.h
	$(CPP) $(CFLAGS) -c hash.cpp

kmv.o: kmv.cpp kmv.h
	$(CPP) $(CFLAGS) -c kmv.cpp

dataobject.o: dataobject.cpp dataobject.h
	$(CPP) $(CFLAGS) -c dataobject.cpp

keyvalue.o: keyvalue.cpp keyvalue.h
	$(CPP) $(CFLAGS) -c keyvalue.cpp

keymultivalue.o: keymultivalue.cpp keymultivalue.h
	$(CPP) $(CFLAGS) -c keymultivalue.cpp

clean: 
	rm *.o *libmrmpi.a
