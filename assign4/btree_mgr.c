#include "btree_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

// for writing metadata
#define META_PAGENO 0
#define BUFFER_FRAME_SIZE 10 // Size of page frame to initialize buffer pool
#define PAGE_REPLACEMENT_ALGO RS_FIFO // Page replacement algorithm to use

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {
	RC retVal = RC_OK;
	initStorageManager();
	return retVal;
}

RC shutdownIndexManager() {
	RC retVal = RC_OK;
	return retVal;
}

// to free BT_Mgmt_Info
RC free_BT_Mgmt_Info(BT_Mgmt_Info* BTMgmtPtr) {
	RC retVal = RC_OK;
	if (BTMgmtPtr != NULL) {
		if (BTMgmtPtr->root != NULL) {
			free(BTMgmtPtr->root);
			BTMgmtPtr->root = NULL;
		}
		free(BTMgmtPtr);
		BTMgmtPtr = NULL;
	}
	return retVal;
}

// to free BTreeHandle
RC freeBTreeHandle(BTreeHandle* tree) {
	RC retVal = RC_OK;
	if (tree != NULL) {
		if (tree->mgmtData != NULL)
			free_BT_Mgmt_Info(tree->mgmtData);
		free(tree);
		tree = NULL;
	}
	return retVal;
}


// create, destroy, open, and close an btree index
RC createBtree(char *idxId, DataType keyType, int n) {
	RC retVal = RC_OK;
	SM_FileHandle fh;
	BTreeHandle BTH;
	BT_Mgmt_Info BTMgmtInfo;

	int intSize = sizeof(int);

	// initialize the basic info
	BTMgmtInfo.order = n;
	BTMgmtInfo.numNodes = 0;
	BTMgmtInfo.firstFreePage = 1;
	// initialize root
	BTMgmtInfo.root = NULL;
	//BTMgmtInfo.root


	// create pagefile for each BTree
	retVal = createPageFile(idxId);
	if (retVal != RC_OK)
	{
		printf("Failed to create pageFile");
		goto END;
	}

	//Open pagefile to write metadata
	retVal = openPageFile(idxId, &fh);
	if (retVal != RC_OK)
	{
		printf("Failed to open pageFile");
		goto END;
	}

	//For writing first page of pagefile with BTree metadata
	char page[PAGE_SIZE];
	char *pagePtr = page;
	// reset buffer content
	memset(pagePtr, 0, PAGE_SIZE);

	*((int*)pagePtr) = BTMgmtInfo.order;
	pagePtr = pagePtr + intSize;
	*((int*)pagePtr) = BTMgmtInfo.numNodes;
	pagePtr = pagePtr + intSize;
	*((int*)pagePtr) = BTMgmtInfo.firstFreePage;
	pagePtr = pagePtr + intSize;

	/*
	*
	*
	* !!!!!!!!!!!!!!!ADD CODE TO WRITE METADATA!!!!!!!!!!!!!!!!!
	*
	*
	*
	*/

	retVal = writeBlock(META_PAGENO, &fh, page);
	if (retVal != RC_OK) {
		printf("Writing block failed\n");
		goto END;
	}


	retVal = closePageFile(&fh);
	if (retVal != RC_OK) {
		printf("close pageFile failed");
		goto END;
	}

END:
	return retVal;
}
RC openBtree(BTreeHandle **tree, char *idxId) {
	RC retVal = RC_OK;
	SM_PageHandle pagePtr;
	BTreeHandle BTH;
	BM_PageHandle bufferPageHandle;
	int intSize = sizeof(int);

	// allocate memory for BTreeHandle
	*tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));
	(*tree)->mgmtData = (BT_Mgmt_Info*)malloc(sizeof(BT_Mgmt_Info));
	((BT_Mgmt_Info*)((*tree)->mgmtData))->root = NULL;
	

	(*tree)->idxId = idxId;
		
	retVal = initBufferPool(&((BT_Mgmt_Info*)((*tree)->mgmtData))->bufferPool, idxId, BUFFER_FRAME_SIZE, PAGE_REPLACEMENT_ALGO, NULL);
	if (retVal != RC_OK) {
		printf("BufferPool initialization failed \n");
		return retVal;
	}

	retVal = pinPage(&((BT_Mgmt_Info*)((*tree)->mgmtData))->bufferPool, &bufferPageHandle, META_PAGENO);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}


	pagePtr = (char*)bufferPageHandle.data;


	((BT_Mgmt_Info*)(*tree)->mgmtData)->order = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;
	((BT_Mgmt_Info*)(*tree)->mgmtData)->numNodes = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;
	((BT_Mgmt_Info*)(*tree)->mgmtData)->firstFreePage = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;

	/*
	*
	*
	*
	*  !!!!!! get back the detail written in first page !!!!!!!!!!!!!!!!!
	*
	*
	*
	*
	*/

	retVal = unpinPage(&((BT_Mgmt_Info*)((*tree)->mgmtData))->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	return retVal;
}
RC closeBtree(BTreeHandle *tree) {
	RC retVal = RC_OK;
	int intSize = sizeof(int);

	BM_PageHandle bufferPageHandle;
	retVal = pinPage(&((BT_Mgmt_Info*)(tree->mgmtData))->bufferPool, &bufferPageHandle, META_PAGENO);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	// while closing the Btree
	char* slotPageData = bufferPageHandle.data;

	// first order data
	slotPageData = slotPageData + intSize;

	// writing number of nodes detail
	*(int*)slotPageData = ((BT_Mgmt_Info*)(tree->mgmtData))->numNodes;


	// update any detail needs to be done
	/*
	
		!!!!!!! write back any detail needs to be written !!!!!!!
	*/

	markDirty(&((BT_Mgmt_Info*)(tree->mgmtData))->bufferPool, &bufferPageHandle);

	retVal = unpinPage(&((BT_Mgmt_Info*)(tree->mgmtData))->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	retVal = shutdownBufferPool(&((BT_Mgmt_Info*)(tree->mgmtData))->bufferPool);
	if (retVal != RC_OK) {
		printf("Shutdown buffer pool failed\n");
		return retVal;
	}

	freeBTreeHandle(tree);

	return retVal;
}
RC deleteBtree(char *idxId) {
	RC retVal = RC_OK;
	retVal = destroyPageFile(idxId);
	if (retVal != RC_OK) {
		printf("destroyPageFile failed");
		return retVal;
	}
	return retVal;
}

// access information about a b-tree
RC getNumNodes(BTreeHandle *tree, int *result) {
	RC retVal = RC_OK;
	return retVal;
}
RC getNumEntries(BTreeHandle *tree, int *result) {
	RC retVal = RC_OK;
	return retVal;
}
RC getKeyType(BTreeHandle *tree, DataType *result) {
	RC retVal = RC_OK;
	return retVal;
}

// index access
RC findKey(BTreeHandle *tree, Value *key, RID *result) {
	RC retVal = RC_OK;
	return retVal;
}
RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
	RC retVal = RC_OK;
	return retVal;
}
RC deleteKey(BTreeHandle *tree, Value *key) {
	RC retVal = RC_OK;
	return retVal;
}
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
	RC retVal = RC_OK;
	return retVal;
}
RC nextEntry(BT_ScanHandle *handle, RID *result) {
	RC retVal = RC_OK;
	return retVal;
}
RC closeTreeScan(BT_ScanHandle *handle) {
	RC retVal = RC_OK;
	return retVal;
}

// debug and test functions
char *printTree(BTreeHandle *tree) {
	RC retVal = RC_OK;
	return retVal;
}