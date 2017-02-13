// Include buffer_mgr.h
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "string.h"

NODE * createNewNode(int pageSlot) {
	NODE * temp = (NODE*)malloc(sizeof(NODE));
	temp->NEXT = NULL;
	temp->frameNumber = pageSlot;
	return temp;
}


int getPageSlotFIFO(BM_BufferPool *const bm) {
	int pageSlot = NO_PAGE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int curIndex = bm->fifoIndex;
	int startPositon = curIndex;
	int totalFrames = bm->numPages;
	mgmtInfo = bm->mgmtData;

	while (mgmtInfo->fixCount[curIndex] != 0 && curIndex != startPositon) {

		if (++curIndex >= totalFrames) {
			curIndex = 0;
		}
	}

	if (curIndex != startPositon) {
		if (mgmtInfo->fixCount[curIndex] == 0) {
			pageSlot = curIndex;
			if (++curIndex >= totalFrames) {
				curIndex = 0;
			}
			bm->fifoIndex = curIndex;
		}
	}
	else {
		printf("No page available");
	}
	return pageSlot;
}


// it will keep adding file to end of the queue
void LRUEnqueue(BM_BufferPool *const bm, int pageSlot) {
	LRUQUEUE* queue = bm->lruQueue;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	mgmtInfo = bm->mgmtData;
	
	// createNewNode will return new node with frameNumber filled with pageSlot
	NODE * temp = createNewNode(pageSlot);

	// queue is empty
	if (queue->allotedTail == NULL) {
		queue->allotedHead = queue->allotedTail = temp;
		return;
	}

	// else we add the new node to rear of the queue
	queue->allotedTail->NEXT = temp;
	queue->allotedTail = temp;

}

int getPageSlotLRU(BM_BufferPool *const bm){

	int pageSlot = NO_PAGE;
	LRUQUEUE* queue = bm->lruQueue;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	mgmtInfo = bm->mgmtData;

	NODE * tempHead = queue->allotedHead;

	// if we are choosing first element in queue
	if (mgmtInfo->fixCount[tempHead->frameNumber] == 0) {
		// move first element in queue to end
		queue->allotedHead = queue->allotedHead->NEXT;
		queue->allotedTail->NEXT = tempHead;
		queue->allotedTail = tempHead;
		tempHead->NEXT = NULL;
		pageSlot = tempHead->frameNumber;
		return pageSlot;
	}

	// search for the page whose count is zero
	while (tempHead->NEXT != NULL && mgmtInfo->fixCount[tempHead->frameNumber] > 0) {
		tempHead = tempHead->NEXT;
	}

	if (tempHead->NEXT == NULL) {
		pageSlot = NO_PAGE;
		return pageSlot;
	}

	if (mgmtInfo->fixCount[tempHead->NEXT->frameNumber] == 0) {
		pageSlot = tempHead->NEXT->frameNumber;
		queue->allotedTail->NEXT = tempHead->NEXT;
		queue->allotedTail = tempHead->NEXT;
		tempHead->NEXT = tempHead->NEXT->NEXT;
		return pageSlot;
	}

}

int getBufferPoolSlot(BM_BufferPool *const bm)
{
	int pageSlot = NO_PAGE;
	

	
	switch(bm->strategy) 
	{
		case RS_FIFO:
			pageSlot = getPageSlotFIFO(bm);
			break; 
  		case RS_LRU:
			pageSlot = getPageSlotLRU(bm);
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
	printf("return value %d",pageSlot);
	return pageSlot;
}

int findPageinPool(BM_BufferPool *const bm, int pageNum)
{
	int pageSlot = NO_PAGE;
	BM_Pool_Mgmt_Info* mgmtInfo = bm->mgmtData;
	
	for(int i = 0; i < bm->numPages; i++)
	{
		if (mgmtInfo->pageNums[i] == pageNum)
		{
			pageSlot = i;
			break;
		}
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
		if(bm->mgmtData != NULL)
			free(bm->mgmtData);
		goto end;
	}
	
	mgmtInfo->pageNums = (PageNumber*)malloc(sizeof(PageNumber) * numPages);
	mgmtInfo->dirtyFlags = (bool*)malloc(sizeof(bool) * numPages);
	mgmtInfo->fixCount = (int*)malloc(sizeof(int) * numPages);
	
	for (int i = 0; i < numPages; i++)
	{
		mgmtInfo->pageNums[i] = NO_PAGE;
		mgmtInfo->fixCount[i] = 0;
		mgmtInfo->dirtyFlags[i] = FALSE;
	}

//	memset((void*)mgmtInfo->dirtyFlags, FALSE, sizeof(bool) * numPages);
	
	mgmtInfo->stratData = stratData;
	mgmtInfo->readCount = 0;
	mgmtInfo->writeCount = 0;
	
	bm->numPages = numPages;
	bm->strategy = strategy;

// kiran init fifo queue for page replacement
	if(bm->strategy == RS_FIFO){
		bm->fifoIndex = 0;
		bm->lruQueue = NULL;
	}

	if (bm->strategy == RS_LRU) {

		bm->lruQueue = (LRUQUEUE*)malloc(sizeof(LRUQUEUE));
		bm->lruQueue->allotedHead = NULL;
		bm->lruQueue->allotedTail = NULL;
		for (int i = 0; i < bm->numPages; i++) {
			LRUEnqueue(bm, i);
		}

	}
// kiran end init fifo queue for page replacement
			
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
	
//kiran Free fifo related memory
	if(bm->strategy == RS_FIFO){


	}

	if (bm->strategy == RS_LRU && bm->lruQueue != NULL) {
		NODE* ittr = bm->lruQueue->allotedHead;
		NODE* tempIttr = NULL;
		while (ittr != NULL) {
			tempIttr = ittr;
			ittr = ittr->NEXT;
			if(tempIttr != NULL)
				free(tempIttr);
		}
		free(bm->lruQueue);
	}
//kiran end Free fifo related memory

	if(mgmtInfo->poolAddr != NULL)
		free(mgmtInfo->poolAddr);
	if(mgmtInfo->poolAddr != NULL)
		free(mgmtInfo->pageNums);
	if(mgmtInfo->poolAddr != NULL)
		free(mgmtInfo->dirtyFlags);
	if(mgmtInfo->poolAddr != NULL)
		free(mgmtInfo->fixCount);
	if(mgmtInfo->poolAddr != NULL)
		free(bm->mgmtData);
	if(mgmtInfo->poolAddr != NULL)
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
		if ((mgmtInfo->dirtyFlags[i] == TRUE) && (mgmtInfo->fixCount[i] == 0))
		{
			ph = (SM_PageHandle)&(mgmtInfo->poolAddr[i]);
			if((retVal = writeBlock (mgmtInfo->pageNums[i], &fh, ph)) != RC_OK)
			{
				goto end;
			}
			mgmtInfo->writeCount += 1;
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
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if ((pageSlot = findPageinPool(bm, page->pageNum)) != NO_PAGE)
	{
		mgmtInfo->dirtyFlags[pageSlot] = TRUE;
	}
	
end:	
	return retVal;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if ((pageSlot = findPageinPool(bm, page->pageNum)) != NO_PAGE)
	{
		/*
		
		if((retVal = forcePage(bm, page)) != RC_OK)
		{
			goto end:
		}
		
		memset((void *)&(mgmtInfo->poolAddr[pageSlot]), 0, PAGE_SIZE);
		mgmtInfo->pageNums[pageSlot] = NO_PAGE;
		mgmtInfo->dirtyFlags[pageSlot] = FALSE;
		
		*/
		mgmtInfo->fixCount[pageSlot]--;
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
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if ((pageSlot = findPageinPool(bm, page->pageNum)) != NO_PAGE)
	{
		//if(mgmtInfo->dirtyFlags[pageSlot] == TRUE)
		{
			if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
			{
				goto end;
			}
			
			pageFileOpen = TRUE;
			
			ph = (SM_PageHandle)&(mgmtInfo->poolAddr[pageSlot]);
			if((retVal = writeBlock (page->pageNum, &fh, ph)) != RC_OK)
			{
				goto end;
			}
			
			mgmtInfo->writeCount += 1;
			mgmtInfo->dirtyFlags[pageSlot] = FALSE;
		}
	}


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
	SM_PageHandle ph;
	bool pageFileOpen = FALSE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if ((pageSlot = findPageinPool(bm, pageNum) != NO_PAGE))
	{
		page->pageNum = pageNum;
		page->data = (char*)&(mgmtInfo->poolAddr[pageSlot]);
	}
	else if((pageSlot = getBufferPoolSlot(bm)) != NO_PAGE)
	{
		// if the selected page is dirty then make sure its written back before data is overwritten
		if (mgmtInfo->dirtyFlags[pageSlot] == TRUE) {
			BM_PageHandle *pageHandleToFlush = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
			pageHandleToFlush->pageNum = pageSlot;
			if ((retVal = forcePage(bm, pageHandleToFlush)) != RC_OK)
			{
				if(pageHandleToFlush != NULL)
					free(pageHandleToFlush);
				goto end;
			}
			if(pageHandleToFlush != NULL)
				free(pageHandleToFlush);
		}
		if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
		{
			goto end;
		}
	
		pageFileOpen = TRUE;
		
		ph = (SM_PageHandle)&(mgmtInfo->poolAddr[pageSlot]);
		
		if((retVal = readBlock (pageNum, &fh, ph)) != RC_OK)
		{
			goto end;
		}
		
		page->pageNum = pageNum;
		page->data = (char*)&(mgmtInfo->poolAddr[pageSlot]);
		mgmtInfo->pageNums[pageSlot] = page->pageNum;
		mgmtInfo->dirtyFlags[pageSlot] = FALSE;
		mgmtInfo->readCount += 1;
	}
	else
	{
		retVal = RC_ERROR_PIN_PAGE;
		RC_message = "Error no page in buffer pool can be replaced";
	}
	
	if(pageSlot != -1)
		mgmtInfo->fixCount[pageSlot]++;
	
end:	
	if(pageFileOpen == TRUE)
		retVal = closePageFile(&fh);
	
	return retVal;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->pageNums;
	
end:	
	return retVal;
}
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->dirtyFlags;
	
end:	
	return retVal;
}
int *getFixCounts (BM_BufferPool *const bm)
{
	int* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->fixCount;
	
end:	
	return retVal;
}
int getNumReadIO (BM_BufferPool *const bm)
{
	int retVal = 0;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->readCount;
	
end:	
	return retVal;
}
int getNumWriteIO (BM_BufferPool *const bm)
{
	int retVal = 0;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->writeCount;
	
end:	
	return retVal;
}
