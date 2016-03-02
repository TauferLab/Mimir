CPP = mpicxx
CFLAGS = -fopenmp -Wall -g -O1 -I../src
LFLAGS = -fopenmp -L ../src 
 
USERLIBS = -lmrmpi
all: wordcount wordcount_m bfs bfs_mm octree_move_lg

wordcount: wordcount.o
	$(CPP) $(CFLAGS) $(LFLAGS) wordcount.o $(USERLIBS) -o wordcount 

wordcount_m: wordcount_m.o
	$(CPP) $(CFLAGS) $(LFLAGS) wordcount_m.o $(USERLIBS) -o wordcount_m

bfs: bfs.o
	$(CPP) $(CFLAGS) $(LFLAGS) bfs.o $(USERLIBS) -o bfs 

bfs_mm: bfs_mm.o
	$(CPP) $(CFLAGS) $(LFLAGS) bfs_mm.o $(USERLIBS) -o bfs_mm

octree_move_lg: octree_move_lg.o
	$(CPP) $(CFLAGS) $(LFLAGS) octree_move_lg.o $(USERLIBS) -o octree_move_lg

clean:
	rm *.o wordcount bfs bfs_mm octree_move_lg

#bfs.o: bfs.cpp
#	$(CPP) $(CFLAGS) -c $<

#bfs.o: bfs.cpp
#	$(CPP) $(CFLAGS) -c $<

bfs_mm.o: bfs.cpp
	$(CPP) $(CFLAGS) -DBFS_MM -o bfs_mm.o -c bfs.cpp

wordcount_m.o: wordcount.cpp
	$(CPP) $(CFLAGS) -DWC_M -o wordcount_m.o -c wordcount.cpp

%.o:%.cpp
	$(CPP) $(CFLAGS) -c $<
