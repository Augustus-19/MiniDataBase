#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include "dberror.h"
#include "tables.h"
#include "buffer_mgr.h"

#define MAX_BTREE_ORDER 511

typedef struct _BT_Node {
	void ** pointers;
	Value ** keys;
	struct _BT_Node * parent;
	bool is_leaf;
	int num_keys;
	struct _BT_Node * next;
} BT_Node;

typedef struct _BT_Data_Node {
	RID rid;
} BT_Data_Node;

typedef struct _BT_Mgmt_Info {
	BM_BufferPool bufferPool;
	BM_PageHandle pageHandler;
	int order;
	int numNodes;
	int numEntries;
	BT_Node * root;
	BT_Node * queue;
	DataType keyType;
} BT_Mgmt_Info;

typedef struct _BT_Scan_Mgmt_Info {
	int keyIndex;
	int totalKeys;
	int order;
	BT_Node * node;
} BT_Scan_Mgmt_Info;

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

// to free BT_Mgmt_Info
extern RC free_BT_Mgmt_Info(BT_Mgmt_Info* BTMgmtPtr);
extern RC freeBTreeHandle(BTreeHandle* tree);

#endif // BTREE_MGR_H
