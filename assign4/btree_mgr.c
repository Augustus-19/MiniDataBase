#include "dberror.h"
#include "tables.h"
#include "storage_mgr.h"
#include "btree_mgr.h"
#include "btree_mgr_helper.h"
#include "buffer_mgr.h"


// This structure stores the metadata for our Index Manager
BT_Mgmt_Info * mgmtInfo = NULL;

// init and shutdown index manager
RC initIndexManager(void *mgmtData)
{
	RC retVal = RC_OK;
	initStorageManager();
	return retVal;
}

// shutdown indexManger
RC shutdownIndexManager()
{
	RC retVal = RC_OK;
	printf("shutdownIndexManager() called");
	mgmtInfo = NULL;
	return retVal;
}

// Open Bplus tree and allocate memory for BTreeHandle and initialize the buffer pool
RC openBtree(BTreeHandle **tree, char *idxId)
{
	
	RC retVal = RC_OK;
	*tree = (BTreeHandle *) malloc(sizeof(BTreeHandle));
	(*tree)->mgmtData = mgmtInfo;

	// Initialize a Buffer Pool using Buffer Manager
	retVal = initBufferPool(&mgmtInfo->bufferPool, idxId, 1000, RS_FIFO, NULL);

	if (retVal != RC_OK) {
		printf("Failed to init buffer poool!!!");

	}
	return retVal;
}

// deallocate and free dynamic allocagted resources

RC closeBtree(BTreeHandle *tree)
{

	RC retVal = RC_OK;
	printf("closeBtree reached");
	
	BT_Mgmt_Info * treeManager = (BT_Mgmt_Info*)tree->mgmtData;

//markPage dirty so changes are flushed back 
	markDirty(&treeManager->bufferPool, &treeManager->pageHandler);

// close buffer pool
	shutdownBufferPool(&treeManager->bufferPool);

// release memory
	free(treeManager);
	free(tree);

	
	return retVal;
}


// Insert the key to bplus tree
RC insertKey(BTreeHandle *tree, Value *key, RID rid)
{
	
	//printf("insertKey ");
	RC retVal = RC_OK;
	BT_Mgmt_Info *treeManager = (BT_Mgmt_Info *)tree->mgmtData;
	BT_Data_Node * pointer;
	BT_Node * leaf;

	int bTreeOrder = treeManager->order;



// check for duplicate value
	// Check is a record with the spcified key already exists.
	if (getDataNode(treeManager->root, key) != NULL) {
		printf("!!!Duplicate insertion");
		retVal = RC_IM_KEY_ALREADY_EXISTS;
		goto END;
	}

	// allocate new record
	pointer = createDataNode(&rid);

	// If the tree doesn't exist yet, create a new tree.
	if (treeManager->root == NULL) {
		// printf("creating first node");
		treeManager->root = createNewTree(treeManager, key, pointer);
		return retVal;
	}

	// Tree already precent find the leaf to add key
	leaf = getLeaf(treeManager->root, key);
	// node has space for one more key
	if (leaf->num_keys < bTreeOrder - 1) {
		//printf("insert key to existing node");
		leaf = leafInsert(treeManager, leaf, key, pointer);
	}
	else {
		// node is full split and insert value

		treeManager->root = leafSplitInsert(treeManager, leaf, key, pointer);
		//printf("split node and insert kin");
	}

END:
	return retVal;
}

// deleteBtree destroy.
RC deleteBtree(char *idxId)
{
	RC retVal = RC_OK;
	//printf("deleteBTree ");
	retVal = destroyPageFile(idxId);
	if (retVal  != RC_OK){
		printf("!!!! should not be here");
	}
	return retVal;
}


// create bplus tree with name idxid and key = keyType with n order
RC createBtree(char *idxId, DataType keyType, int n)
{
	RC retVal = RC_OK;
	int maxNodes = PAGE_SIZE / sizeof(BT_Node);

	// Return error if we cannot accommodate a B++ Tree of that order.
	if (n > maxNodes) {
		printf("\n n = %d > Max. Nodes = %d \n", n, maxNodes);
		retVal = RC_ORDER_TOO_HIGH_FOR_PAGE;
		goto END;
	}

	// Initialize Bplus tree metadata detail.
	// order, number of nodes, number of entries, root node, queue
	mgmtInfo = (BT_Mgmt_Info *)malloc(sizeof(BT_Mgmt_Info));
	mgmtInfo->order = n + 2;
	mgmtInfo->numNodes = 0;
	mgmtInfo->numEntries = 0;
	mgmtInfo->root = NULL;
	mgmtInfo->queue = NULL;
	mgmtInfo->keyType = keyType;

// allocate memory for bufferpool
	BM_BufferPool * bm = (BM_BufferPool *) malloc(sizeof(BM_BufferPool));
	mgmtInfo->bufferPool = *bm;

	SM_FileHandle fileHandler;
	RC result;

	char data[PAGE_SIZE];
	char *pagePtr = data;
	// reset buffer content
	memset(pagePtr, 0, PAGE_SIZE);

	*((int*)pagePtr) = mgmtInfo->order;
	pagePtr = pagePtr + sizeof(int);
	*((int*)pagePtr) = mgmtInfo->numNodes;
	pagePtr = pagePtr + sizeof(int);


// createPageFile with the bplus tree name
	if ((retVal = createPageFile(idxId)) != RC_OK){
		goto END;
	}


// open pagefile
	if ((retVal = openPageFile(idxId, &fileHandler)) != RC_OK){
		goto END;
	}


// write bplus tree manager data to page 0
	if ((retVal = writeBlock(0, &fileHandler, data)) != RC_OK){
		goto END;
	}

// close page file
	if ((retVal = closePageFile(&fileHandler)) != RC_OK){
		goto END;
	}

//if any error we will code will jump to END label and returns
END:
	return retVal;
}

// get the key type used
RC getKeyType(BTreeHandle *tree, DataType *result)
{

	RC retVal = RC_OK;
	BT_Mgmt_Info * treeManager = (BT_Mgmt_Info *)tree->mgmtData;

	//print("getNumEntries %d",treeManager->keyType);
	*result = treeManager->keyType;
	return retVal;
}


// get the node count in bplus tree

RC getNumNodes(BTreeHandle *tree, int *result)
{

	RC retVal = RC_OK;
	BT_Mgmt_Info * treeManager = (BT_Mgmt_Info *)tree->mgmtData;


	*result = treeManager->numNodes;
	//printf("node count %d",treeManager->numNodes);
	return retVal;
}

// find the key in tree
extern RC findKey(BTreeHandle *tree, Value *key, RID *result)
{

	RC retVal = RC_OK;
	BT_Mgmt_Info *treeManager = (BT_Mgmt_Info *)tree->mgmtData;

	// find key
	BT_Data_Node * r = getDataNode(treeManager->root, key);

	// key wont exists
	if (r == NULL) {
		return RC_IM_KEY_NOT_FOUND;
	}
	else {
	
		//printf("found key--->%d in page--->%d and slot----->%d \n",key->v.intV,  r->rid.page,r->rid.slot);
	}


// give the record id
	*result = r->rid;
END:
	return retVal;
}

// delete key from tree
RC deleteKey(BTreeHandle *tree, Value *key)
{

	RC retVal = RC_OK;
	// printf("deleteKey");
	BT_Mgmt_Info *treeManager = (BT_Mgmt_Info *)tree->mgmtData;

	// get updated root
	treeManager->root = deleteKeyFromTree(treeManager, key);
	return retVal;
}

// get the key entry count
RC getNumEntries(BTreeHandle *tree, int *result)
{
	
	RC retVal = RC_OK;
	//print("getNumEntries");
	BT_Mgmt_Info * treeManager = (BT_Mgmt_Info *)tree->mgmtData;
	//print("getNumEntries =====> %d",treeManager->numEntries);


	*result = treeManager->numEntries;
	return retVal;
}

//opentreescan
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{

	RC retVal = RC_OK;
	BT_Mgmt_Info *treeManager = (BT_Mgmt_Info *)tree->mgmtData;


	BT_Scan_Mgmt_Info *scanmeta = malloc(sizeof(BT_Scan_Mgmt_Info));


	*handle = malloc(sizeof(BT_ScanHandle));

	BT_Node * node = treeManager->root;

	if (treeManager->root == NULL) {
		retVal = RC_NO_RECORDS_TO_SCAN;
		goto END;
	}
	else {

		while (!node->is_leaf)
			node = node->pointers[0];

		// initialize data
		scanmeta->keyIndex = 0;
		scanmeta->totalKeys = node->num_keys;
		scanmeta->node = node;
		scanmeta->order = treeManager->order;
		(*handle)->mgmtData = scanmeta;
		
	}
END:
	return retVal;
}



// get nextentry 
RC nextEntry(BT_ScanHandle *handle, RID *result)
{

	//printf("nextEntry(BT_ScanHandle *handle, RID *result) ");
	
	RC retVal = RC_OK;

	BT_Scan_Mgmt_Info * scanmeta = (BT_Scan_Mgmt_Info *)handle->mgmtData;

	// get keyindex and total keys
	int keyIndex = scanmeta->keyIndex;
	int totalKeys = scanmeta->totalKeys;
	int bTreeOrder = scanmeta->order;
	RID rid;


	BT_Node * node = scanmeta->node;
	
		//printf(" key = %d and #of keys = %d ",keyIndex,totalKeys);

	// Return error if current node is empty i.e. NULL
	if (node == NULL) {
		retVal = RC_IM_NO_MORE_ENTRIES;
		goto END;
	}

	if (keyIndex < totalKeys) {

		rid = ((BT_Data_Node *)node->pointers[keyIndex])->rid;

		scanmeta->keyIndex++;
	}
	else {
		// current node is done go for next node
		if (node->pointers[bTreeOrder - 1] != NULL) {
			node = node->pointers[bTreeOrder - 1];
			scanmeta->keyIndex = 1;
			scanmeta->totalKeys = node->num_keys;
			scanmeta->node = node;
			rid = ((BT_Data_Node *)node->pointers[0])->rid;
			//printf("  pagenumber =>%d slotid ===> %d  ", rid.page, rid.slot);
		}
		else {
			// no entires to scan
			retVal = RC_IM_NO_MORE_ENTRIES;
			goto END;
		}
	}

	*result = rid;
	
END:
	return retVal;
}

// close tree scan
extern RC closeTreeScan(BT_ScanHandle *handle)
{

	RC retVal = RC_OK;
	//printf("closeTreeScan");
	handle->mgmtData = NULL;
	free(handle);
	return retVal;
}

