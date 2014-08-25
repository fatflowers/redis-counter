objs = sds.o zmalloc.o main.o rediscounter.o
CC = gcc
CFLAGS = -g -std=c99 -pedantic -Wall -W -fPIC
all: $(objs) 
	@echo "--------------------------compile start here---------------------------------"
	$(CC) $(CFLAGS) -o redis-counter $(objs) -lm 
	@echo "--------------------------compile  end  here---------------------------------"

main.o: main.c rediscounter.h
sds.o: sds.c sds.h zmalloc.h
zmalloc.o: zmalloc.c config.h zmalloc.h
rediscounter.o: rediscounter.c rediscounter.h sds.h zmalloc.h
clean:
	-rm *.o redis-counter
