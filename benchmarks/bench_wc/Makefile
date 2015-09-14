CC = mpicc
CPP = mpic++
CLINK = mpicc
CPPLINK = mpic++
DEBUG = -g
CFLAGS = -fopenmp -std=c++11 -Wall $(DEBUG) -O1 -I../src
LFLAGS = -fopenmp -L../src
USERLIBS = -lmrmpi
OBJS = wordcount.o

all: wordcount

wordcount: $(OBJS)
	$(CPPLINK) $(LFLAGS) wordcount.o $(USERLIBS) -o wordcount
wordcount.o: wordcount.cpp
	$(CPP) $(CFLAGS) -c wordcount.cpp

clean:
	rm *.o wordcount
