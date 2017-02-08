// Include buffer_mgr.h
#include "buffer_mgr.h"

//Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	return RC_OK;
}
RC forceFlushPool(BM_BufferPool *const bm)
{
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	return RC_OK;
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	return RC_OK;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	return RC_OK;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	return RC_OK;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{

}
int *getFixCounts (BM_BufferPool *const bm)
{
	
}
int getNumReadIO (BM_BufferPool *const bm)
{
	
}
int getNumWriteIO (BM_BufferPool *const bm)
{
	
}
