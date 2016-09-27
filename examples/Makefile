ifeq ($(CC),tau)
  CPP=tau_cxx.sh
  CPPLINK=tau_cxx.sh
else
  CPP=mpicxx
  CPPLINK=mpicxx
endif
CC=openmp
SPI_CFLAGS=-DBGQ=1 -I/bgsys/drivers/ppcfloor -I/bgsys/drivers/ppcfloor/spi/include/kernel/cnk
SPI_LDFLAGS=-L/bgsys/drivers/ppcfloor/spi/lib -lSPI_cnk
ifeq ($(CC),mp)
  LCFLAGS = -fopenmp -g -O -DMPICH_IGNORE_CXX_SEEK -I../src $(SPI_CFLAGS)
  LFLAGS = -fopenmp -L ../src  $(SPI_LDFALGS)
else
  LCFLAGS = -g -O -DMPICH_IGNORE_CXX_SEEK -I../src $(SPI_CFLAGS)
  LFLAGS = -L ../src $(SPI_LDFLAGS)
endif

USERLIBS = -lmrmpi

all: wordcount bfs octree_move_lg

wordcount: wordcount.o
	$(CPP) $(LCFLAGS) $(CFLAGS) $(LFLAGS) wordcount.o $(USERLIBS) -o wordcount

#wordcount_m: wordcount_m.o
#	$(CPP) $(CFLAGS) $(LFLAGS) wordcount_m.o $(USERLIBS) -o wordcount_m

bfs: bfs.o
	$(CPP) $(LCFLAGS) $(CFLAGS) $(LFLAGS) bfs.o $(USERLIBS) -o bfs 

#bfs_mm: bfs_mm.o
#	$(CPP) $(CFLAGS) $(LFLAGS) bfs_mm.o $(USERLIBS) -o bfs_mm

octree: octree_move_lg.o
	$(CPPLINK) $(CFLAGS) $(CFLAGS) $(LFLAGS) octree_move_lg.o $(USERLIBS) -o octree

clean:
	rm *.o wordcount bfs octree

#bfs.o: bfs.cpp
#	$(CPP) $(CFLAGS) -c $<

#bfs.o: bfs.cpp
#	$(CPP) $(CFLAGS) -c $<

#bfs_mm.o: bfs.cpp
#	$(CPP) $(CFLAGS) -DBFS_MM -o bfs_mm.o -c bfs.cpp

#wordcount_m.o: wordcount.cpp
#	$(CPP) $(CFLAGS) -DWC_M -o wordcount_m.o -c wordcount.cpp

%.o:%.cpp
	$(CPP) $(LCFLAGS) $(CFLAGS) -c $<
