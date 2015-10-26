CC = mpicc
CPP = mpic++
CLINK = mpicc
CPPLINK = mpic++
DEBUG = -g
CFLAGS = -fopenmp -std=c++11 -Wall $(DEBUG) -O2 -I../../src
LFLAGS = -fopenmp -L../../src
USERLIBS = -lmrmpi
OBJS = octree_move_lg.o

all: octree_move_lg

octree_move_lg: $(OBJS)
	$(CPPLINK) $(LFLAGS) octree_move_lg.o $(USERLIBS) -o octree_move_lg
octree_move_lg.o: octree_move_lg.cpp
	$(CPP) $(CFLAGS) -c octree_move_lg.cpp

clean:
	rm *.o octree_move_lg
