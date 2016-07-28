ifeq ($(CC),tau)
  CC = tau_cc.sh
  CPP = tau_cxx.sh
  CLINK = tau_cc.sh
  CPPLINK = tau_cxx.sh
else
  CC = mpicc
  CPP = mpicxx
  CLINK = mpicc
  CPPLINK = mpicxx
endif
DEBUG = -g
local_CFLAGS = -fopenmp $(DEBUG) -O2  -DENABLE_PROFILER -DENABLE_TRACKER
#local_CFLAGS = $(DEBUG) -O3  -DENABLE_PROFILER -DENABLE_TRACKER
#local_CFLAGS = $(DEBUG) -O2
LFLAGS = 
ARCHIEVE = ar
AFLAGS =  -rcv
OBJS = mapreduce.o dataobject.o keyvalue.o keymultivalue.o communicator.o alltoall.o  ptop.o hash.o spool.o const.o memory.o stat.o  

all: libmrmpi

libmrmpi: $(OBJS)
	$(ARCHIEVE) $(AFLAGS) libmrmpi.a $(OBJS)

mapreduce.o: mapreduce.cpp mapreduce.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c mapreduce.cpp

dataobject.o: dataobject.cpp dataobject.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c dataobject.cpp

keyvalue.o: keyvalue.cpp keyvalue.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c keyvalue.cpp

keymultivalue.o: keymultivalue.cpp keymultivalue.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c keymultivalue.cpp

communicator.o: communicator.cpp communicator.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c communicator.cpp

alltoall.o: alltoall.cpp alltoall.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c alltoall.cpp

ptop.o: ptop.cpp ptop.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c ptop.cpp

hash.o: hash.cpp hash.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c hash.cpp

spool.o: spool.cpp spool.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c spool.cpp

const.o: const.cpp const.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c const.cpp

memory.o: memory.cpp memory.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c memory.cpp

stat.o: stat.cpp stat.h
	$(CPP) $(local_CFLAGS) $(CFLAGS) -c stat.cpp

clean: 
	rm *.o libmrmpi.a
