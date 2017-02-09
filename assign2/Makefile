THIS_FILE := $(lastword $(MAKEFILE_LIST))
.PHONY: all test1 test2 clean common
CC=gcc
CFLAGS=-Wall

COMMON_OBJS=dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o
TEST1_OBJS=$(COMMON_OBJS) test_assign2_1.o
TEST2_OBJS=$(COMMON_OBJS) test_assign2_2.o
ALL_OBJS=$(COMMON_OBJS) test_assign2_1.o test_assign2_2.o

TEST1_OUT=test_assign2_1
TEST2_OUT=test_assign2_2
ALL_OUTS=$(TEST1_OUT) $(TEST2_OUT)
	
all:
	$(MAKE) -f $(THIS_FILE) test1
	$(MAKE) -f $(THIS_FILE) test2
#	$(eval OBJS=$(ALL_OBJS))
#	$(eval OUT=$(ALL_OUTS))

test1: 
	$(MAKE) -f $(THIS_FILE) common
	$(CC) $(CFLAGS) -c test_assign2_1.c
	$(CC) -o $(TEST1_OUT) $(TEST1_OBJS)
#	$(eval OBJS=$(TEST1_OBJS))
#	$(eval OUT=$(TEST1_OUT))
	
test2: 
	$(MAKE) -f $(THIS_FILE) common
	$(CC) $(CFLAGS) -c test_assign2_2.c
	$(CC) -o $(TEST2_OUT) $(TEST2_OBJS)
#	$(eval OBJS=$(TEST2_OBJS))
#	$(eval OUT=$(TEST2_OUT))
	
common: 
	$(CC) $(CFLAGS) -c dberror.c
	$(CC) $(CFLAGS) -c storage_mgr.c
	$(CC) $(CFLAGS) -c buffer_mgr.c
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c


clean:
	rm -f $(ALL_OUTS) $(ALL_OBJS)
