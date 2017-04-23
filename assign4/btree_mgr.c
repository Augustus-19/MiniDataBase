#include "btree_mgr.h"
#include "dberror.h"

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {
	RC retVal = RC_OK;
	return retVal;
}

RC shutdownIndexManager() {
	RC retVal = RC_OK;
	return retVal;
}

// create, destroy, open, and close an btree index
RC createBtree(char *idxId, DataType keyType, int n) {
	RC retVal = RC_OK;
	return retVal;
}
RC openBtree(BTreeHandle **tree, char *idxId) {
	RC retVal = RC_OK;
	return retVal;
}
RC closeBtree(BTreeHandle *tree) {
	RC retVal = RC_OK;
	return retVal;
}
RC deleteBtree(char *idxId) {
	RC retVal = RC_OK;
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