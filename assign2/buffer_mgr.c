// Include buffer_mgr.h
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "string.h"

int getBufferPoolSlot(BM_BufferPool *const bm)
{
	int pageSlot = NO_PAGE;
	
	//if()
	
	switch(bm->strategy) 
	{
		case RS_FIFO:
			break; 
  		case RS_LRU:
  			break;
  		case RS_CLOCK:
  			break;
  		case RS_LFU:
  			break;
  		case RS_LRU_K:
  			break;
  		default:
  			break;
	}
	
	return pageSlot;
}


//Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	RC retVal = RC_OK;
	SM_FileHandle fh;
	bool pageFileOpen = FALSE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	if((retVal = openPageFile(pageFileName, &fh)) != RC_OK)
	{
		goto end;
	}
	
	pageFileOpen = TRUE;
	
	if(fh.totalNumPages < numPages)
	{
		retVal = RC_INVALID_BUFFER_POOL_SIZE;
		goto end;
	}
		
	bm->pageFile = malloc(sizeof(char) * (strlen(pageFileName) + 1));
	bm->mgmtData = malloc(sizeof(BM_Pool_Mgmt_Info));
	mgmtInfo = bm->mgmtData;
	
	mgmtInfo->poolAddr = (PAGE_FRAME*)malloc(sizeof(PAGE_FRAME) * numPages);
	
	if(mgmtInfo->poolAddr == NULL)
	{
		retVal = RC_INSUFFICIENT_MEMORY;
		free(bm->mgmtData);
		goto end;
	}
	
	mgmtInfo->pageNums = (PageNumber*)malloc(sizeof(PageNumber) * numPages);
	mgmtInfo->dirtyFlags = (bool*)malloc(sizeof(bool) * numPages);
	
	memset((void*)mgmtInfo->pageNums, sizeof(PageNumber) * numPages, 0);
	memset((void*)mgmtInfo->dirtyFlags, sizeof(bool) * numPages, FALSE);
	
	mgmtInfo->stratData = stratData;
	mgmtInfo->readCount = 0;
	mgmtInfo->writeCount = 0;
	
	bm->numPages = numPages;
	bm->strategy = strategy;
	strcpy(bm->pageFile, pageFileName);
end:	
	if(pageFileOpen == TRUE)
		retVal = closePageFile(&fh);

	return retVal;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if((retVal = forceFlushPool(bm)) != RC_OK )
	{
		goto end;
	}
	
	free(mgmtInfo->poolAddr);
	free(mgmtInfo->pageNums);
	free(mgmtInfo->dirtyFlags);
	free(bm->mgmtData);
	free(bm->pageFile);
	
end:	
	return retVal;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	SM_FileHandle fh;
  	SM_PageHandle ph;
  	bool pageFileOpen = FALSE;
	
	if(bm == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
	{
		goto end;
	}
	
	pageFileOpen = TRUE;
	
	for(int i = 0; i < bm->numPages; i++)
	{
		if (mgmtInfo->dirtyFlags[i] == TRUE)
		{
			ph = (SM_PageHandle)&(mgmtInfo->poolAddr[i]);
			if((retVal = writeBlock (mgmtInfo->pageNums[i], &fh, ph)) != RC_OK)
			{
				goto end;
			}
			mgmtInfo->dirtyFlags[i] = FALSE;
		}
	}
	
end:	
	if(pageFileOpen == TRUE)
		retVal = closePageFile(&fh);
	
	return retVal;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
end:	
	return retVal;
}
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
end:	
	return retVal;
}
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	SM_FileHandle fh;
  	SM_PageHandle ph;
  	bool pageFileOpen = FALSE;
	int pageSlot = -1;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	for(int i = 0; i < bm->numPages; i++)
	{
		if (mgmtInfo->pageNums[i] == TRUE)
		{
			ph = (SM_PageHandle)&(mgmtInfo->poolAddr[i]);
			if((retVal = writeBlock (mgmtInfo->pageNums[i], &fh, ph)) != RC_OK)
			{
				goto end;
			}
			mgmtInfo->dirtyFlags[i] = FALSE;
		}
	}
	
	if(mgmtInfo->dirtyFlags[page->pageNum] == TRUE)
	{
		
	}
	
	if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
	{
		goto end;
	}
	
	pageFileOpen = TRUE;
	
end:	
	if(pageFileOpen == TRUE)
		retVal = closePageFile(&fh);
	return retVal;
}
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	RC retVal = RC_OK;
	SM_FileHandle fh;
	bool pageFileOpen = FALSE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
	{
		goto end;
	}
	
	pageFileOpen = TRUE;
	
	//if(
	
	
	
end:	
	if(pageFileOpen == TRUE)
		retVal = closePageFile(&fh);
	
	return retVal;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	return NULL;
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	return NULL;
}
int *getFixCounts (BM_BufferPool *const bm)
{
	return NULL;
}
int getNumReadIO (BM_BufferPool *const bm)
{
	return 0;
}
int getNumWriteIO (BM_BufferPool *const bm)
{
	return 0;
}
