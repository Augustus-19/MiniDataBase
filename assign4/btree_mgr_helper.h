#ifndef BTREE_MGR_HELPER_H
#define BTREE_MGR_HELPER_H

#include "btree_mgr.h"

// Create Operations
BT_Node * createNewTree(BT_Mgmt_Info * mgmtInfo, Value * key, BT_Data_Node * pointer);
BT_Node * createIntNode(BT_Mgmt_Info * mgmtInfo);
BT_Node * createLeafNode(BT_Mgmt_Info * mgmtInfo);
BT_Data_Node * createDataNode(RID * rid);

//Insert Operations

BT_Node * leafInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * leaf, Value * key, BT_Data_Node * pointer);
BT_Node * leafSplitInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * leaf, Value * key, BT_Data_Node * pointer);
BT_Node * intNodeInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * parent, int left_index, Value * key, BT_Node * right);
BT_Node * intNodeSplitInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * parent, int left_index, Value * key, BT_Node * right);
BT_Node * ancestorInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * left, Value * key, BT_Node * right);
BT_Node * newRootInsert(BT_Mgmt_Info * mgmtInfo, BT_Node * left, Value * key, BT_Node * right);

// Comparison functions
bool lesserThan(Value * val1, Value * val2);
bool greaterThan(Value * val1, Value * val2);
bool areEqual(Value * val1, Value * val2);

// Find Operations
BT_Node * getLeaf(BT_Node * root, Value * key);
BT_Data_Node * getDataNode(BT_Node * root, Value * key);

// Delete, merge and Redistribute operations
BT_Node * replaceRoot(BT_Node * root);
BT_Node * mergeNodes(BT_Mgmt_Info * mgmtInfo, BT_Node * n, BT_Node * neighbor, int neighbor_index, int k_prime);
BT_Node * redistNode(BT_Node * root, BT_Node * n, BT_Node * neighbor, int neighbor_index, int k_prime_index, int k_prime);
BT_Node * deleteDataNode(BT_Mgmt_Info * mgmtInfo, BT_Node * n, Value * key, void * pointer);
BT_Node * deleteKeyFromTree(BT_Mgmt_Info * mgmtInfo, Value * key);
BT_Node * removeKeyFromNode(BT_Mgmt_Info * mgmtInfo, BT_Node * n, Value * key, BT_Node * pointer);


// Other
int getLeftIndex(BT_Node * node, BT_Node * left);
int getNeighborIndex(BT_Node * n);



#endif // BTREE_MGR_HELPER_H
