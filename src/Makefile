CC = mpicc
CPP = mpic++
CLINK = mpicc
CPPLINK = mpic++
DEBUG = -g
CFLAGS = -fopenmp -std=c++11  $(DEBUG) -O2  -DDEBUG=0 -DFUSION
LFLAGS = 
ARCHIEVE = ar
AFLAGS =  -rcv
OBJS = mapreduce.o dataobject.o keyvalue.o keymultivalue.o communicator.o hash.o spool.o 

all: libmrmpi

libmrmpi: $(OBJS)
	$(ARCHIEVE) $(AFLAGS) libmrmpi.a $(OBJS)

mapreduce.o: mapreduce.cpp mapreduce.h
	$(CPP) $(CFLAGS) -c mapreduce.cpp

dataobject.o: dataobject.cpp dataobject.h
	$(CPP) $(CFLAGS) -c dataobject.cpp

keyvalue.o: keyvalue.cpp keyvalue.h
	$(CPP) $(CFLAGS) -c keyvalue.cpp

keymultivalue.o: keymultivalue.cpp keymultivalue.h
	$(CPP) $(CFLAGS) -c keymultivalue.cpp

communicator.o: communicator.cpp communicator.h
	$(CPP) $(CFLAGS) -c communicator.cpp

hash.o: hash.cpp hash.h
	$(CPP) $(CFLAGS) -c hash.cpp

spool.o: spool.cpp spool.h
	$(CPP) $(CFLAGS) -c spool.cpp

clean: 
	rm *.o *libmrmpi.a
