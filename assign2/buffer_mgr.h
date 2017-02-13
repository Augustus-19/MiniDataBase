#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
  RS_FIFO = 0,
  RS_LRU = 1,
  RS_CLOCK = 2,
  RS_LFU = 3,
  RS_LRU_K = 4
} ReplacementStrategy;


// replacement strategy structures
// node with page frame and next data info
typedef struct Node{
  int frameNumber;
  struct Node * NEXT;
}NODE;

// Queue structure which will hold head and tail of the queue
// Queue structure used for LRU algo
typedef struct LRUQueue {
	NODE * allotedHead;
	NODE * allotedTail;
}LRUQUEUE;
// end of replacement strategy structures

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct _PAGE_FRAME{
	char frame[PAGE_SIZE];
}PAGE_FRAME;

typedef struct _BM_Pool_Mgmt_Info {
  PAGE_FRAME * poolAddr; //address of buffer pool
  PageNumber* pageNums;
  bool* dirtyFlags;
  int* fixCount;
  int readCount;
  int writeCount;
  void * stratData;
} BM_Pool_Mgmt_Info;

typedef struct BM_BufferPool {
  char *pageFile;
  int numPages;
  ReplacementStrategy strategy;
  LRUQUEUE * lruQueue;
  int fifoIndex;  // used for fifo
  // 0 evict if 1 clear to 0 then advance
  int *clockEvictionRef;
  int clockIndex;
  void *mgmtData; // use this one to store the bookkeeping info your buffer 
                  // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
  PageNumber pageNum;
  char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
  ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
  ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// page replacement
int getPageSlotFIFO(BM_BufferPool *const bm);
int getPageSlotLRU(BM_BufferPool *const bm);
int getPageSlotCLOCK(BM_BufferPool *const bm);

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
