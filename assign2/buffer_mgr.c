// Include buffer_mgr.h
#include <stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "string.h"

NODE * createNewNode(int pageSlot) 
{
	NODE * temp = (NODE*)malloc(sizeof(NODE));
	temp->NEXT = NULL;
	temp->frameNumber = pageSlot;
	return temp;
}


void changeEvictionBit(BM_BufferPool *const bm,int curIndex) {

	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	mgmtInfo = bm->mgmtData;
	// flip the clock index
	if (mgmtInfo->fixCount[curIndex] == 0 && bm->clockEvictionRef[curIndex] == 1) {
		bm->clockEvictionRef[curIndex] = 0;
	}else if (mgmtInfo->fixCount[curIndex] == 1 && bm->clockEvictionRef[curIndex] == 0) {
		// do nothing
	}else if (mgmtInfo->fixCount[curIndex] == 1 && bm->clockEvictionRef[curIndex] == 1) {
		bm->clockEvictionRef[curIndex] = 0;
	}
}

int getPageSlotCLOCK(BM_BufferPool *const bm) {
	int remaining_rounds = 2;
	int pageSlot = NO_PAGE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int curIndex = bm->clockIndex;
	int startPositon = curIndex;
	int totalFrames = bm->numPages;
	mgmtInfo = bm->mgmtData;

	if (mgmtInfo->fixCount[curIndex] == 0 && bm->clockEvictionRef[curIndex] == 0) {
		pageSlot = curIndex;
		bm->clockEvictionRef[curIndex] = 1;  // once page selected mark it as referenced
		if (++curIndex >= totalFrames) {
			curIndex = 0;
		}
		bm->clockIndex = curIndex;
		return pageSlot;
	}
	else {

		changeEvictionBit(bm, curIndex);

		++curIndex;
		if (curIndex >= totalFrames) {
			curIndex = 0;
		}

		while (remaining_rounds > 0 ) {
			if (curIndex == startPositon)
				remaining_rounds--;

			if (mgmtInfo->fixCount[curIndex] == 0 && bm->clockEvictionRef[curIndex] == 0) {
				pageSlot = curIndex;
				bm->clockEvictionRef[curIndex] = 1;  // once page selected mark it as referenced
				if (++curIndex >= totalFrames) {
					curIndex = 0;
				}
				bm->clockIndex = curIndex;
				return pageSlot;
			}else
			changeEvictionBit(bm, curIndex);
			
			if (++curIndex >= totalFrames) {
				curIndex = 0;
			}
		}

	}

	return pageSlot;
}

int getPageSlotFIFO(BM_BufferPool *const bm) 
{
	int pageSlot = NO_PAGE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int curIndex = bm->fifoIndex;
	int startPositon = curIndex;
	int totalFrames = bm->numPages;
	mgmtInfo = bm->mgmtData;

	// to check first element
	if (mgmtInfo->fixCount[curIndex] == 0) {
		pageSlot = curIndex;
		if (++curIndex >= totalFrames) {
			curIndex = 0;
		}
		bm->fifoIndex = curIndex;
		return pageSlot;
	}
	else {
		++curIndex;
		if (curIndex >= totalFrames) {
			curIndex = 0;
		}

		// if first frame is not free then we check all other frames
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
	}
	
	return pageSlot;
}


// it will keep adding file to end of the queue
void LRUEnqueue(BM_BufferPool *const bm, int pageSlot) 
{
	LRUQUEUE* queue = bm->lruQueue;
	
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

void markPageFrameMarked(BM_BufferPool *const bm, int pageFrame) {

	LRUQUEUE* queue = bm->lruQueue;

	NODE * tempHead = queue->allotedHead;

	if (tempHead->frameNumber == pageFrame) {
		// move first element in queue to end
		queue->allotedHead = queue->allotedHead->NEXT;
		queue->allotedTail->NEXT = tempHead;
		queue->allotedTail = tempHead;
		tempHead->NEXT = NULL;
		return;
	}

	while ((tempHead->NEXT != NULL) && (tempHead->NEXT->frameNumber != pageFrame)) {
		tempHead = tempHead->NEXT;
	}

	if (tempHead->NEXT == NULL) {
		printf("Should Not enter here");
		return;
	}

	if (tempHead->NEXT->frameNumber == pageFrame) {
		queue->allotedTail->NEXT = tempHead->NEXT;
		queue->allotedTail = tempHead->NEXT;
		tempHead->NEXT = tempHead->NEXT->NEXT;
		queue->allotedTail->NEXT = NULL;
	}
	else {
		
	}

	return;
}

int getPageSlotLRU(BM_BufferPool *const bm){

	int pageSlot = NO_PAGE;
	LRUQUEUE* queue = bm->lruQueue;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	NODE * tempHead = queue->allotedHead;
	mgmtInfo = bm->mgmtData;

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
	while ((tempHead->NEXT != NULL) && (mgmtInfo->fixCount[tempHead->NEXT->frameNumber] > 0)) {
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
		queue->allotedTail->NEXT = NULL;
		
	}
	else {
		pageSlot = NO_PAGE;
	}


	return pageSlot;
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
			pageSlot = getPageSlotCLOCK(bm);
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

int findPageinPool(BM_BufferPool *const bm, int pageNum)
{
	int pageSlot = NO_PAGE;
	BM_Pool_Mgmt_Info* mgmtInfo = bm->mgmtData;
	int i;
	
	for(i = 0; i < bm->numPages; i++)
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
	int i;
	
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
	
	for (i = 0; i < numPages; i++)
	{
		mgmtInfo->pageNums[i] = NO_PAGE;
		mgmtInfo->fixCount[i] = 0;
		mgmtInfo->dirtyFlags[i] = FALSE;
	}
	
	mgmtInfo->stratData = stratData;
	mgmtInfo->readCount = 0;
	mgmtInfo->writeCount = 0;
	
	bm->numPages = numPages;
	bm->strategy = strategy;

	// Init fifo queue for page replacement
	if(bm->strategy == RS_FIFO){
		bm->fifoIndex = 0;
		bm->lruQueue = NULL;
		bm->clockEvictionRef = NULL;
	} else if (bm->strategy == RS_CLOCK) {
		// for clock replacement algo set initially to 0
		bm->clockEvictionRef = (int*)malloc(sizeof(int)*numPages);
		for (i = 0; i < numPages; i++)
			bm->clockEvictionRef[i] = 0;
		bm->clockIndex = 0;
	} else if (bm->strategy == RS_LRU) {

		bm->lruQueue = (LRUQUEUE*)malloc(sizeof(LRUQUEUE));
		bm->lruQueue->allotedHead = NULL;
		bm->lruQueue->allotedTail = NULL;
		for (i = 0; i < bm->numPages; i++) {
			LRUEnqueue(bm, i);
		}

		bm->clockEvictionRef = NULL;
	}
			
	strcpy(bm->pageFile, pageFileName);
end:	
	if(pageFileOpen == TRUE)
		closePageFile(&fh);

	return retVal;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int i;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	//Check if there are any pinned pages
	for(i = 0; i < bm->numPages; i++)
	{
		if (mgmtInfo->fixCount[i] != 0)
		{
			retVal = RC_ERROR_SHUTDOWN_BUFFER_POOL;
			goto end;
		}
	}
	
	//Force flush all dirty pages in pool before shutdown
	if((retVal = forceFlushPool(bm)) != RC_OK )
	{
		goto end;
	}
	
	//Free FIFO related memory
	if(bm->strategy == RS_FIFO){

	}

	//Free LRU related memory
	if (bm->strategy == RS_LRU && bm != NULL && bm->lruQueue != NULL) {
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

	if (mgmtInfo->poolAddr != NULL) {
		free(mgmtInfo->poolAddr);
		mgmtInfo->poolAddr = NULL;
	}
		
	if (mgmtInfo->pageNums != NULL) {
		free(mgmtInfo->pageNums);
		mgmtInfo->pageNums = NULL;
	}
		
	if (mgmtInfo->dirtyFlags != NULL) {
		free(mgmtInfo->dirtyFlags);
		mgmtInfo->dirtyFlags = NULL;
	}
		
	if (mgmtInfo->fixCount != NULL) {
		free(mgmtInfo->fixCount);
		mgmtInfo->fixCount = NULL;
	}
		
	if (bm->mgmtData != NULL) {
		free(bm->mgmtData);
		bm->mgmtData = NULL;
	}
		
	if (bm->pageFile != NULL) {
		free(bm->pageFile);
		bm->pageFile = NULL;
	}
		
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
  	int i;
	
	if(bm == NULL || bm->pageFile == NULL || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
	{
		retVal = RC_CANNOT_FLUSH_BUFFER_POOL;
		goto end;
	}
	
	pageFileOpen = TRUE;
	
	for(i = 0; i < bm->numPages; i++)
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
		closePageFile(&fh);
	
	return retVal;
}

// Buffer Manager Interface Access Pages

//Mark page in buffer pool as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	//Set dirty flag of page to true if found in buffer pool
	if ((pageSlot = findPageinPool(bm, page->pageNum)) != NO_PAGE)
	{
		mgmtInfo->dirtyFlags[pageSlot] = TRUE;
	}
	else
	{
		retVal = RC_ERROR_MARKING_DIRTY;
	}
	
end:	
	return retVal;
}

//Unpin page from buffer pool
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	//Decrement fix count of page by 1 if found in buffer pool and is not 0
	if ((pageSlot = findPageinPool(bm, page->pageNum)) != NO_PAGE)
	{	
		if (mgmtInfo->fixCount[pageSlot] > 0)
			mgmtInfo->fixCount[pageSlot]--;
	}
	else
	{
		retVal = RC_ERROR_UNPIN_PAGE;
	}
	
end:	
	return retVal;
}

//Force write the page in buffer to pagefile
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	RC retVal = RC_OK;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	SM_FileHandle fh;
  	SM_PageHandle ph;
  	bool pageFileOpen = FALSE;
	int pageSlot = NO_PAGE;
	
	if(bm == NULL || page == NULL || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	//Write the data at the given page slot to page file
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
	else
	{
		retVal = RC_ERROR_FORCING_PAGE;
	}


end:	
	if(pageFileOpen == TRUE)
		closePageFile(&fh);
	return retVal;
}

//Pin page to buffer pool, ensuring that page is appended to page file if necessary 
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	RC retVal = RC_OK;
	SM_FileHandle fh;
	SM_PageHandle ph;
	bool pageFileOpen = FALSE;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	int pageSlot = NO_PAGE;
	BM_PageHandle *pageHandleToFlush = NULL;
	
	if(bm == NULL || page == NULL || pageNum < 0 || bm->mgmtData == NULL)
	{
		retVal = RC_NULL_PARAM;
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	if ((pageSlot = findPageinPool(bm, pageNum)) != NO_PAGE)
	{
		page->pageNum = pageNum;
		page->data = (char*)&(mgmtInfo->poolAddr[pageSlot]);
		if (bm->strategy == RS_LRU && bm != NULL && bm->lruQueue != NULL) {
			markPageFrameMarked(bm, pageSlot);
		}
	}
	else if((pageSlot = getBufferPoolSlot(bm)) != NO_PAGE)
	{
		// if the selected page is dirty then make sure its written back before data is overwritten
		if (mgmtInfo->dirtyFlags[pageSlot] == TRUE) 
		{
			pageHandleToFlush = (BM_PageHandle*)malloc(sizeof(BM_PageHandle));
			pageHandleToFlush->pageNum = mgmtInfo->pageNums[pageSlot];
			
			if ((retVal = forcePage(bm, pageHandleToFlush)) != RC_OK)
			{
				if(pageHandleToFlush != NULL)
					free(pageHandleToFlush);
				goto end;
			}
			if(pageHandleToFlush != NULL)
				free(pageHandleToFlush);
		}
		
		//Open page file and read the required page into buffer
		if((retVal = openPageFile(bm->pageFile, &fh)) != RC_OK)
		{
			goto end;
		}
	
		pageFileOpen = TRUE;
		
		if((retVal = ensureCapacity((int)pageNum + 1, &fh)) != RC_OK)
		{
			goto end;
		}
		
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
		mgmtInfo->fixCount[pageSlot] = 0;
	}
	else
	{
		//Error no page in buffer pool can be replaced
		retVal = RC_ERROR_PIN_PAGE;
	}
	
	if(pageSlot != NO_PAGE)
		mgmtInfo->fixCount[pageSlot]++;

end:	
	if(pageFileOpen == TRUE)
		closePageFile(&fh);
	
	return retVal;
}

// Statistics Interface

//Get page numbers of pages in buffer pool
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->pageNums;
	
end:	
	return retVal;
}

//Get dirty flags of pages in buffer pool
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->dirtyFlags;
	
end:	
	return retVal;
}

//Get fix counts of pages in buffer pool
int *getFixCounts (BM_BufferPool *const bm)
{
	int* retVal = NULL;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->fixCount;
	
end:	
	return retVal;
}

//Get number of reads to page file
int getNumReadIO (BM_BufferPool *const bm)
{
	int retVal = 0;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->readCount;
	
end:	
	return retVal;
}

//Get number of writes to page file
int getNumWriteIO (BM_BufferPool *const bm)
{
	int retVal = 0;
	BM_Pool_Mgmt_Info* mgmtInfo = NULL;
	
	if(bm == NULL || bm->mgmtData == NULL)
	{
		goto end;	
	}
	
	mgmtInfo = bm->mgmtData;
	
	retVal = mgmtInfo->writeCount;
	
end:	
	return retVal;
}
