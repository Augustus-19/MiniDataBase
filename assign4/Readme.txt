ASSIGNMENT 4:B+-Tree

OBJECTIVE: To implement a B+-tree index which is backed up by a page file and pages of the index should be accessed through the buffer manager.


TEAM MEMBERS:
1. Kiran C D
2. Sashank B V
3. Adarsh S


FILES INCLUDED:

Makefile
buffer_mgr.c
buffer_mgr.h
buffer_mgr_stat.c
buffer_mgr_stat.h
btree_mgr.c
btree_mgr.h
btree_mgr_helper.c
btree_mgr_helper.h
dberror.c
dberror.h
dt.h
expr.c
expr.h
record_mgr.c
record_mgr.h
rm_serializer.c
storage_mgr.h
tables.h
test_assign4_1.c
test_expr.c
test_helper.h


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

HOW TO BUILD THE PROJECT:
	1) run "make all" from the assign4 directory, this will create output files "test_assign4_1"
	2) run "make clean" from the assign4 directory to delete all files created by previous build


HOW TO RUN THE PROJECT:
	1) Build the project using description in previous section
	2) run "./test_assign4_1" to run the testcases defined in the test assign file 
	3) run "./test_expr" to run the testcases defined in the test expr file 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


FUNCTION DESCRIPTION:


SL NO 						NAME				DESCRIPTION
---------------------------------------------------------------------------------------------------------------------------------------------------------------------

1.					initIndexManager			Initializes the B+ tree Index Manager		

2.					shutdownIndexManager 			Shutdown the B+ tree Index Manager			

3.					free_BT_Mgmt_Info 			This function is to free the value stored in BT_Mgmt_Info	

4.					freeBTreeHandle 			This function is to free the value stored in BTreeHandle	

5.					createBtree				Initialize B+ Tree by creating page file, initialize Buffer pool, Pin the page and 											update the btree info whenever a root is created.
		
6.					openBtree				It reads the page from buffer and prepare the schema and unpin the page after 											reading

7.					closeBtree 				This function frees the memory occupied by the B+ tree				


8.					deleteBtree 				This function deletes the B+ tree by removing the corresponding page file 					

9.					getNumNodes 				This function gets the total number of nodes in the B+ tree


10.					getNumEntries 				This function gets the total number of keys in the B+ tree			

11.					getKeyType 				This function gets the datatype of the key			


12.					findKey 				This function searches for the given key in the B+ tree	and returns return 											RC_KEY_NOT_FOUND on failure			

13.					insertKey 				This function checks for the number of nodes, if it is equal to zero start the new 											tree else find the node to insert the key. It checks for the number of keys in the 											node and if it is less than the maximum number of keys at node insert into leaf 								
14.					deleteKey 				Deletes the given key and checks for underflow and then it redistributes the node and 											update the parent	


15.					openTreeScan				It Scans through all the entries in the B+ tree		

16.					nextEntry				Find the next entry in the B+ tree using the B+ tree and Return RC_NO_MORE_ENTRIES on 											failure or RC_OK on success 											

17.					closeTreeScan 				frees the memory occupied by scanhandle			


18.					printTree 				creates and displays a string representation of a B+ tree and return RC_OK 

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DESIGN DESCRIPTION

We have implemented a B+- Tree in which a pages are accessed through our buffer manager.  The B+-tree stores pointer to recordsindex by a keys of a given datatype.We have created B+- tree functions  to create,delete and also to remove corresponding b-tree index. We have ensured that all new or modified pages of the index are flushed back to disk after closing a b-tree. Our function insertKey inserts a new key and record pointer pair into the index and returns error code RC_IM_KEY_ALREADY_EXISTS if this key is already stored in the b-tree. Our another function deleteKey removes a key from the index and returns RC_IM_KEY_NOT_FOUND if the key is not in the index. openTreeScan, nextEntry, and closeTreeScan functions are used to scan through the entries of a btree. The function nextEntry method will return RC_IM_NO_MORE_ENTRIES if there are no more entries to be returned.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TESTCASES

SL NO	Objective						Observed result							Expected result
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
1.	test b-tree inserting and search			The number of entries and the number of nodes are checked	Insertion and searching of a b-tree 
								and RID is searched successfully				should happen successfully

2.	select some entries for deletion 			Some of the random entries are removed successfully		Entries should be deleted permanently
																and should be non retrivable. 

3.	random insertion order and scan				Inserted into B+-tree and the entries are retrived 		B+-tree insertion should be successful

4.	Create permutation of b-tree values and free them	Created a permutation of btree values and are freed		Create a permutation of btree values
																and the memory should be freeable

5.	test value serialization and deserialization		Serialization and deserialization of values are done		Correctly serialize and deserialize
								successfully.							the test values

6.	test value comparison and boolean operators		Boolean operators and values are compared successfully		Compare the boolean operators and 																	test values

7.	test complex expressions				Some of the complex values are tested successfully		Test some of the complex expressions

8.	checks the return code and exit if it's and error	The return code is checked for any errors and exits		An error in the return code should 																	result in an exit

9.	check whether two strings are equal			Checks two strings and returns if they are equal		Checks for the equality of two strings

10.	check whether two ints are equals			Checks two integers and returns if they are equal		Checks for the equality of two 																	integers

11.	check that a method returns an error code		The return code of a method is checked for any errors 		An error in the return code should 				
							
