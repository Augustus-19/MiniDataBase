.PHONY: all clean
CC=gcc
CFLAGS=-Wall
OBJS=dberror.o storage_mgr.o test_assign1_1.o

all: 
	$(CC) $(CFLAGS) -c dberror.c
	$(CC) $(CFLAGS) -c storage_mgr.c
	$(CC) $(CFLAGS) -c test_assign1_1.c
	$(CC) -o a.out $(OBJS)


clean:
	rm a.out $(OBJS)
