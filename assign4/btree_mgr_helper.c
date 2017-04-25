#include "dt.h"
#include "string.h"
#include "btree_mgr_helper.h"
// create operations
//create new tree with first node
BT_Node * createNewTree(BT_Mgmt_Info * treeManager, Value * key, BT_Data_Node * pointer)
{
	BT_Node * root = createLeafNode(treeManager);
	int bTreeOrder = treeManager->order;

	root->keys[0] = key;
	root->pointers[0] = pointer;
	root->pointers[bTreeOrder - 1] = NULL;
	root->parent = NULL;
	root->num_keys++;

	treeManager->numEntries++;

	return root;
}

// Creates a new general node, which can be adapted to serve as either a leaf or an internal node.
BT_Node * createIntNode(BT_Mgmt_Info * treeManager)
{
	treeManager->numNodes++;
	int bTreeOrder = treeManager->order;

	BT_Node * new_node = malloc(sizeof(BT_Node));
	// failed to alloc new node
	if (new_node == NULL) {
		printf("node creation failed.");
		exit(RC_INSERT_ERROR);
	}

// allocate memory for keys
	new_node->keys = malloc((bTreeOrder - 1) * sizeof(Value *));
	if (new_node->keys == NULL) {
		printf("new node key is null");
		exit(RC_INSERT_ERROR);
	}

// allocate memory for pointers
	new_node->pointers = malloc(bTreeOrder * sizeof(void *));
	if (new_node->pointers == NULL) {
		printf("New node pointers array.");
		exit(RC_INSERT_ERROR);
	}

	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
	return new_node;
}

// create leaf node
BT_Node * createLeafNode(BT_Mgmt_Info * treeManager)
{
    //printf("createLeafNode()");
	BT_Node * leaf = createIntNode(treeManager);
	leaf->is_leaf = true;
	return leaf;
}


// create dataNode to hold keys and pointers
BT_Data_Node * createDataNode(RID * rid)
{
	//printf("createDataNode()");
	BT_Data_Node * record = (BT_Data_Node *)malloc(sizeof(BT_Data_Node));
	if (record == NULL) {
		perror("NodeData creation.");
		exit(RC_INSERT_ERROR);
	}
	// 
	else {
		record->rid.page = rid->page;
		record->rid.slot = rid->slot;
	}
	return record;
}



// insert item to leaf, if data inserted inbetween whole data is moved
BT_Node * leafInsert(BT_Mgmt_Info * treeManager, BT_Node * leaf, Value * key, BT_Data_Node * pointer)
{
	int i, insertion_point;
	treeManager->numEntries++;
	
	// printf("leafInsert");

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && lesserThan(leaf->keys[insertion_point], key))
		insertion_point++;

	// kiran move forward
	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	
	
	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = pointer;
	leaf->num_keys++;
	return leaf;
}


// insert new key by splitting leaf
BT_Node * leafSplitInsert(BT_Mgmt_Info * treeManager, BT_Node * leaf, Value * key, BT_Data_Node * pointer)
{
	BT_Node * new_leaf;
	Value ** temp_keys;
	void ** temp_pointers;
	int insertion_index, split, new_key, i, j;
	
	//printf("leafSplitInsert()");

	new_leaf = createLeafNode(treeManager);
	int bTreeOrder = treeManager->order;


// memory for keys
	temp_keys = malloc(bTreeOrder * sizeof(Value));
	if (temp_keys == NULL) {
		perror("Temporary keys array.");
		exit(RC_INSERT_ERROR);
	}

// memory for pointers
	temp_pointers = malloc(bTreeOrder * sizeof(void *));
	if (temp_pointers == NULL) {
		perror("Temporary pointers array.");
		exit(RC_INSERT_ERROR);
	}


	insertion_index = 0;
	// find the position to insert
	while (insertion_index < bTreeOrder - 1 && lesserThan(leaf->keys[insertion_index], key))
		insertion_index++;


	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index)
			j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = pointer;

	leaf->num_keys = 0;

// position to split

	if ((bTreeOrder - 1) % 2 == 0)
		split = (bTreeOrder - 1) / 2;
	else
		split = (bTreeOrder - 1) / 2 + 1;

// copy back THE DATA
	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < bTreeOrder; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

// after copying free the temp variables
	free(temp_pointers);
	free(temp_keys);

	new_leaf->pointers[bTreeOrder - 1] = leaf->pointers[bTreeOrder - 1];
	leaf->pointers[bTreeOrder - 1] = new_leaf;

	for (i = leaf->num_keys; i < bTreeOrder - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < bTreeOrder - 1; i++)
		new_leaf->pointers[i] = NULL;

	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];
	treeManager->numEntries++;
	// recursive call to insert
	return ancestorInsert(treeManager, leaf, new_key, new_leaf);
}


// insert key if node have space
BT_Node * intNodeInsert(BT_Mgmt_Info * treeManager, BT_Node * parent, int left_index, Value * key, BT_Node * right)
{
	int i;
	// printf("intNodeInsert");
	for (i = parent->num_keys; i > left_index; i--) {
		parent->pointers[i + 1] = parent->pointers[i];
		parent->keys[i] = parent->keys[i - 1];
	}

	parent->pointers[left_index + 1] = right;
	parent->keys[left_index] = key;
	parent->num_keys++;

// returning root
	return treeManager->root;
}

// split the node and insert data
BT_Node * intNodeSplitInsert(BT_Mgmt_Info * treeManager, BT_Node * old_node, int left_index, Value * key, BT_Node * right)
{

	// printf("intNodeSplitInsert ");
	int i, j, split, k_prime;
	BT_Node * new_node, *child;
	Value ** temp_keys;
	BT_Node ** temp_pointers;

	int bTreeOrder = treeManager->order;


	temp_pointers = malloc((bTreeOrder + 1) * sizeof(BT_Node *));
	if (temp_pointers == NULL) {
		perror("Temporary pointers array for splitting nodes.");
		exit(RC_INSERT_ERROR);
	}
	temp_keys = malloc(bTreeOrder * sizeof(Value *));
	if (temp_keys == NULL) {
		perror("Temporary keys array for splitting nodes.");
		exit(RC_INSERT_ERROR);
	}

	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1)
			j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index)
			j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;


// create split and copy old value and new key
	if ((bTreeOrder - 1) % 2 == 0)
		split = (bTreeOrder - 1) / 2;
	else
		split = (bTreeOrder - 1) / 2 + 1;

	new_node = createIntNode(treeManager);
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < bTreeOrder; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}

// add old node to left new to the rightS

	treeManager->numEntries++;
	return ancestorInsert(treeManager, old_node, k_prime, new_node);
}

// parent node will be inserted and final root node returned at the end
BT_Node * ancestorInsert(BT_Mgmt_Info * treeManager, BT_Node * left, Value * key, BT_Node * right)
{

	//printf("ancestorInsert ");
	int left_index;
	BT_Node * parent = left->parent;
	int bTreeOrder = treeManager->order;

	// if this is the first node
	if (parent == NULL)
		return newRootInsert(treeManager, left, key, right);

	// get the parent
	left_index = getLeftIndex(parent, left);

	// still space is there in thenode, insert in existing node
	if (parent->num_keys < bTreeOrder - 1) {
		return intNodeInsert(treeManager, parent, left_index, key, right);
	}

	// if none of above cases are reached then need to split and insert key
	return intNodeSplitInsert(treeManager, parent, left_index, key, right);
}

// create new root for the two subtrees
BT_Node * newRootInsert(BT_Mgmt_Info * treeManager, BT_Node * left, Value * key, BT_Node * right)
{
	BT_Node * root = createIntNode(treeManager);
	root->keys[0] = key;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->num_keys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}

//Comparison functions

// check values for comparision
bool lesserThan(Value * val1, Value * val2)
{

	// printf("lesserThan");
	bool retVal = FALSE;

	switch (val1->dt)
	{
		case DT_INT:
		// int data type
			retVal = (val1->v.intV < val2->v.intV)? TRUE : FALSE;
			break;
		case DT_FLOAT:
		// float data type
			retVal = (val1->v.floatV < val2->v.floatV)? TRUE : FALSE;
			break;
		case DT_STRING:
		// for string
			retVal = (strcmp(val1->v.stringV, val2->v.stringV) == -1)? TRUE : FALSE;
			break;
		case DT_BOOL:
		// for boolean value
			retVal = FALSE;
			break;
	}

	return retVal;
}

// greater comparision
bool greaterThan(Value * val1, Value * val2)
{

	// printf("greater comparision");
	bool retVal = FALSE;

	switch (val1->dt)
	{
		case DT_INT:
		// int data type
			retVal = (val1->v.intV > val2->v.intV)? TRUE : FALSE;
			break;
		case DT_FLOAT:
			retVal = (val1->v.floatV > val2->v.floatV)? TRUE : FALSE;
				// float data type
			break;
		case DT_STRING:
			retVal = (strcmp(val1->v.stringV, val2->v.stringV) == 1)? TRUE : FALSE;
			// for string
			break;
		case DT_BOOL:
			retVal = FALSE;
			// for boolean value
			break;
	}

	return retVal;
}


// check for equality
bool areEqual(Value * val1, Value * val2)
{
	bool retVal = FALSE;

	switch (val1->dt)
	{
		case DT_INT:
			retVal = (val1->v.intV == val2->v.intV)? TRUE : FALSE;
			// int data type
			break;
		case DT_FLOAT:
		// float data type
			retVal = (val1->v.floatV == val2->v.floatV)? TRUE : FALSE;
			break;
		case DT_STRING:
		// for string
			retVal = (strcmp(val1->v.stringV, val2->v.stringV) == 0)? TRUE : FALSE;
			break;
		case DT_BOOL:
			retVal = (val1->v.boolV == val2->v.boolV)? TRUE : FALSE;
			// for boolean value
			break;
	}

	return retVal;
}



// Find Operations

// find the key get from root to leaf
BT_Node * getLeaf(BT_Node * root, Value * key)
{
	int i = 0;
	// printf("getLeaf");
	BT_Node * c = root;
	if (c == NULL) {
		return c;
	}
	
	// go through nodes till leaf is reached
	while (!c->is_leaf) {
		i = 0;
		while (i < c->num_keys) {
			if (greaterThan(key, c->keys[i]) || areEqual(key, c->keys[i])) {
				i++;
			}
			else
				break;
		}
		c = (BT_Node *)c->pointers[i];
	}
	return c;
}

// find the key and return corresponding node
BT_Data_Node * getDataNode(BT_Node * root, Value *key)
{
	int i = 0;
	//printf("getDataNode");
	BT_Node * c = getLeaf(root, key);
	if (c == NULL)
		return NULL;
	for (i = 0; i < c->num_keys; i++) {
		if (areEqual(c->keys[i], key))
			break;
	}
	if (i == c->num_keys)
		return NULL;
	else
		return (BT_Data_Node *)c->pointers[i];
}



// Delete, merge and Redistribute operations

// remove the record with key
BT_Node * removeKeyFromNode(BT_Mgmt_Info * treeManager, BT_Node * n, Value * key, BT_Node * pointer)
{

	int i, num_pointers;
	int bTreeOrder = treeManager->order;
	//printf("removeKeyFromNode");

	// delete key and move other keys
	i = 0;

	while (!areEqual(n->keys[i], key))
		i++;

	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

// fidn number of pointers
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];

	//we have one less key
	n->num_keys--;
	treeManager->numEntries--;

	// set other unused pointers to null
	if (n->is_leaf)
		for (i = n->num_keys; i < bTreeOrder - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->num_keys + 1; i < bTreeOrder; i++)
			n->pointers[i] = NULL;

	return n;
}

// if record deleted it will be replaced
BT_Node * replaceRoot(BT_Node * root)
{

	BT_Node * new_root;

// if root still has data then nothing needs to be done
	if (root->num_keys > 0)
		return root;

	if (!root->is_leaf) {
		
		// considering first child as root
		new_root = root->pointers[0];
		new_root->parent = NULL;
	}
	else {
		// tree is null
		new_root = NULL;
	}

	// remove memory data
	free(root->keys);
	free(root->pointers);
	free(root);

	return new_root;
}

// merge the nodes if they have less values based on order
BT_Node * mergeNodes(BT_Mgmt_Info * treeManager, BT_Node * n, BT_Node * neighbor, int neighbor_index, int k_prime)
{


	//printf("mergeNodes");
	int i, j, neighbor_insertion_index, n_end;
	BT_Node * tmp;
	int bTreeOrder = treeManager->order;

	// excahnge the nodes 
	if (neighbor_index == -1) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	// printf("neighbor->num_keys"):
	neighbor_insertion_index = neighbor->num_keys;

// append all pointer
	if (!n->is_leaf) {
		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->num_keys++;

		n_end = n->num_keys;

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
		}

		neighbor->pointers[i] = n->pointers[j];

		// connecting all child to same parent.
		for (i = 0; i < neighbor->num_keys + 1; i++) {
			tmp = (BT_Node *)neighbor->pointers[i];
			tmp->parent = neighbor;
		}
	}
	else {
		// append to right neighbours
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		neighbor->pointers[bTreeOrder - 1] = n->pointers[bTreeOrder - 1];
	}

	treeManager->root = deleteDataNode(treeManager, n->parent, k_prime, n);

	// free all dynamically allocated memory
	free(n->keys);
	free(n->pointers);
	free(n);
	return treeManager->root;
}

// delete datanode
BT_Node * deleteDataNode(BT_Mgmt_Info * treeManager, BT_Node * n, Value * key, void * pointer)
{
	int min_keys;
	BT_Node * neighbor;
	int neighbor_index;
	int k_prime_index, k_prime;
	int capacity;
	int bTreeOrder = treeManager->order;
	
	// printf("deleteDataNode");

	// remove key with pointer
	n = removeKeyFromNode(treeManager, n, key, pointer);

	// replace if we have n roots
	if (n == treeManager->root)
		return replaceRoot(treeManager->root);

	// find minium size.
	if (n->is_leaf) {
		if ((bTreeOrder - 1) % 2 == 0)
			min_keys = (bTreeOrder - 1) / 2;
		else
			min_keys = (bTreeOrder - 1) / 2 + 1;
	}
	else {
		if ((bTreeOrder) % 2 == 0)
			min_keys = (bTreeOrder) / 2;
		else
			min_keys = (bTreeOrder) / 2 + 1;
		min_keys--;
	}

	
	if (n->num_keys >= min_keys)
		return treeManager->root;


	neighbor_index = getNeighborIndex(n);
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor =
			(neighbor_index == -1) ? n->parent->pointers[1] : n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? bTreeOrder : bTreeOrder - 1;

	if (neighbor->num_keys + n->num_keys < capacity)
		// merging nodes
		return mergeNodes(treeManager, n, neighbor, neighbor_index, k_prime);
	else
		// nodes are redistibuted
		return redistNode(treeManager->root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}

// delete key form the tree
BT_Node * deleteKeyFromTree(BT_Mgmt_Info * treeManager, Value * key)
{

	//printf("deleteKeyFromTree");
	BT_Node * record = getDataNode(treeManager->root, key);
	BT_Data_Node * key_leaf = getLeaf(treeManager->root, key);

	if (record != NULL && key_leaf != NULL) {
		treeManager->root = deleteDataNode(treeManager, key_leaf, key, record);
		free(record);
	}

	return treeManager->root;
}


// redistribute node
BT_Node * redistNode(BT_Node * root, BT_Node * n, BT_Node * neighbor, int neighbor_index, int k_prime_index, int k_prime)
{
	int i;
	BT_Node * tmp;

	//printf("redistNode");
	if (neighbor_index != -1) {

		if (!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		for (i = n->num_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			tmp = (BT_Node *)n->pointers[0];
			tmp->parent = n;
			neighbor->pointers[neighbor->num_keys] = NULL;
			n->keys[0] = k_prime;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		}
		else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			n->parent->keys[k_prime_index] = n->keys[0];
		}
	}
	else {
	
		if (n->is_leaf) {
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		}
		else {
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			tmp = (BT_Node *)n->pointers[n->num_keys + 1];
			tmp->parent = n;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->num_keys - 1; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}

	n->num_keys++;
	neighbor->num_keys--;

	return root;
}


//Other

// get the left index
int getLeftIndex(BT_Node * parent, BT_Node * left)
{
	int left_index = 0;
	while (left_index <= parent->num_keys && parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

// get the neighbor index
int getNeighborIndex(BT_Node * n) {

	int i;


	for (i = 0; i <= n->parent->num_keys; i++)
		if (n->parent->pointers[i] == n)
			return i - 1;


	exit(RC_RECORD_NOT_FOUND);
}

