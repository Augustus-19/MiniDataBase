THIS_FILE := $(lastword $(MAKEFILE_LIST))
.PHONY: all test1 testexpr clean common
CC=gcc
CFLAGS=-Wall

COMMON_OBJS=dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o expr.o rm_serializer.o
TEST1_OBJS=$(COMMON_OBJS) test_assign3_1.o
TESTEXPR_OBJS=$(COMMON_OBJS) test_expr.o
ALL_OBJS=$(COMMON_OBJS) test_assign3_1.o test_expr.o

TEST1_OUT=test_assign3_1
TEST_EXPR=test_expr
ALL_OUTS=$(TEST1_OUT) $(TEST_EXPR)
	
all:
	$(MAKE) -f $(THIS_FILE) test1
#	$(MAKE) -f $(THIS_FILE) testexpr

test1: 
	$(MAKE) -f $(THIS_FILE) common
	$(CC) $(CFLAGS) -c test_assign3_1.c
	$(CC) -o $(TEST1_OUT) $(TEST1_OBJS)
	
testexpr: 
	$(MAKE) -f $(THIS_FILE) common
	$(CC) $(CFLAGS) -c test_expr.c
	$(CC) -o $(TEST_EXPR) $(TESTEXPR_OBJS)
	
common: 
	$(CC) $(CFLAGS) -c dberror.c
	$(CC) $(CFLAGS) -c storage_mgr.c
	$(CC) $(CFLAGS) -c buffer_mgr.c
	$(CC) $(CFLAGS) -c buffer_mgr_stat.c
	$(CC) $(CFLAGS) -c record_mgr.c
	$(CC) $(CFLAGS) -c rm_serializer.c
	$(CC) $(CFLAGS) -c expr.c


clean:
	rm -f $(ALL_OUTS) $(ALL_OBJS)
