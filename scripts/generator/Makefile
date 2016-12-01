CPP=mpicxx
CPPLINK=mpicxx
LCFLAGS = -g -O -Wall -std=c++11 -Wconversion -I../../src
LFLAGS = -L ../../src

USERLIBS = -lmrmpi

all: gen_uniform_words gen_3d_points

gen_uniform_words: gen_uniform_words.o
	$(CPP) $(LCFLAGS) $(LFLAGS) gen_uniform_words.o $(USERLIBS) -o gen_uniform_words

gen_3d_points: gen_3d_points.o
	$(CPP) $(LCFLAGS) $(LFLAGS) gen_3d_points.o $(USERLIBS) -o gen_3d_points


clean:
	-rm *.o gen_uniform_words gen_3d_points

%.o:%.cpp
	$(CPP) $(LCFLAGS) $(CFLAGS) -c $<
