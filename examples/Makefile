CPP = mpic++
CFLAGS = -fopenmp -std=c++11 -Wall -g -O1 -I../src
LFLAGS = -fopenmp -L../src
USERLIBS = -lmrmpi
all: wordcount bfs octree_move_lg

wordcount: wordcount.o
	$(CPP) $(CFLAGS) $(LFLAGS) wordcount.o $(USERLIBS) -o wordcount 

bfs: bfs.o
	$(CPP) $(CFLAGS) $(LFLAGS) bfs.o $(USERLIBS) -o bfs 

octree_move_lg: octree_move_lg.o
	$(CPP) $(CFLAGS) $(LFLAGS) octree_move_lg.o $(USERLIBS) -o octree_move_lg

clean:
	rm *.o wordcount bfs octree_move_lg

%.o:%.cpp
	$(CPP) $(CFLAGS) -c $<
