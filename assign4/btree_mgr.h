#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"
#include "buffer_mgr.h"

#define MAX_BTREE_ORDER 511

typedef struct _BT_Node {
	int numKeys;
	int keys[MAX_BTREE_ORDER];
	int pointers[MAX_BTREE_ORDER + 1];
} BT_Node;

typedef struct _BT_Mgmt_Info {
	int order;
	int numNodes;
	BT_Node * root;
	int firstFreePage;
	BM_BufferPool bufferPool;
} BT_Mgmt_Info;

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

// to free BT_Mgmt_Info
extern RC free_BT_Mgmt_Info(BT_Mgmt_Info* BTMgmtPtr);
extern RC freeBTreeHandle(BTreeHandle* tree);

#endif // BTREE_MGR_H
