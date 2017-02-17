ASSIGNMENT 2: Buffer Manager

OBJECTIVE: To implement a buffer manager that manages a fixed number of pages in memory that represent pages from a page file managed by the storage manager.


TEAM MEMBERS:
1. Kiran C D
2. Sashank B V
3. Adarsh S


FILES INCLUDED:

buffer_mgr.c
buffer_mgr.h
buffer_mgr_stat.c
buffer_mgr_stat.h
dberror.c
dberror.h
dt.h
Makefile
storage_mgr.c
storage_mgr.h
test_assign2_1.c
test_assign2_2.c
test_helper.h


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

HOW TO BUILD THE PROJECT:
	1) run "make all" from the assign2 directory, this will create output files "test_assign2_1" "test_assign2_2"
	2) run "make clean" from the assign2 directory to delete all files created by previous build


HOW TO RUN THE PROJECT:
	1) Build the project using description in previous section
	2) run "./test_assign2_1" to run the testcases defined in the test assign file 1
	3) run "./test_assign2_2" to run the testcases defined in the test assign file 1


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~-


FUNCTION DESCRIPTION:


SL NO 		NAME				DESCRIPTION
----------------------------------------------------------------------------------------------------------------------------------------------
1.		initBufferPool			This function is used to create a buffer pool for an existing page file

2.		shutdownBufferPool		This function is used to shutdown a buffer pool and free up all associated resources

3.		forceFlushPool			This function is used to force the buffer manager to write all dirty pages to disk 

4.		pinPage				This function pins the page with page number pageNum

5.		unpinPage			This function unpins the page

6.		markDirty			This function marks a page as dirty

7.		forcePage			This function writes the current content of the page back to the page file on disk

8.		findPageinPool			This function returns a particular page which is in the buffer poo

9.		getFrameContents		This function returns an array of PageNumbers (of size numPages) where the ith element is the 							number of the page stored in the ith page frame

10.		getFixCounts			This function returns an array of ints (of size numPages) where the ith element is the fix 							count of the page stored in the ith page frame

11.		getNumReadIO			This function returns the number of pages that have been read from disk since a buffer pool 		 					has been initialized

12.		getNumWriteIO			This function returns the number of pages written to the page file since the buffer pool has 							been initialized

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
DESIGN DESCRIPTION & FUTURE WORK:
In our implementation we have added FIFO ,LRU and CLOCK page replacement policies.
Buffer pool is created at in contigeous memory location and the postion of the buffer location is used for deciding which page to replace.
FIFO: First In First Out - 
In this method we have maintained the postion of the last read frame from the buffer. If the position count grows more than the total count of
the frame, position count will start form 0th position.
LRU: Least Recently Used -
In this method we have used queue of singly linked list to keep track of the least recently used pages. Every time page is referenced, buffer page reference is put to the end of the  singly linked list. So least recently used pages are always placed in the head of the queue and recently used files are kept in the tail of the queue. Every time new page request received queue is scanned fromt he head of the queue and alloted files are returned.
CLOCK:
In this method all the page is evicted on second time.
If reference bit is 0 then page will be evicted and if 1 then page is given second chance by just changing reference bit to 0.
Page will be replaced only if the fix count is 0 and reference bit is 0.

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TESTCASES

SL NO	Objective						Observed result					Expected result
-----------------------------------------------------------------------------------------------------------------------------------------------
1.	Create 'n' pages with some content and read them 	Created data and read data matches		Created data and read 	
	back to check whether the content is right								data should match

2.	Testing FIFO replacement strategy by replacing 		FIFO replacement strategy works			FIFO replacement strategy
	random data from the buffer pool									should work


FIFO page replacement strategy testcases 															

3. 	Reading some pages linearly to check the pool content	Able to read the pool content			Pool content must be accessible

4.	Pin a page and check for the pool content		Able to access the pool content after		Pool content must be  									pin page					accessible after pin page


5.	Reading the pages from the pool and marking them	Pages from the pool are marked as dirty		Pages should be marked	
	as dirty												as dirty

6. 	flushing the buffer pool to the disk			Contents of the buffer pool are flushed		Buffer pool should be flushed
								to the disk					to the disk

7.	Check the pool contents(read write I/Os) after		Read and write I/Os are verified 		Verify the read/write I/Os
	flush

8.	Test the LRU page replacement strategy			LRU page replacement strategy works		LRU page replacement strategy
														should work as expected

LRU page replacement strategy testcases


9.	To read the first five pages linearly with direct	Able to read the pool content			Able to read the pool 	
	unpin 													content	

10.	Read the pages to change LRU order			Pages are read to change the LRU order		Pages should be read 															sucessfully so as to change 															the LRU order


11.	Replacing the pages and checking for LRU strategy	LRU strategy is working after replacing 	LRU strategy should work
								the pages

		
12.	To check the number of IOs and validating them		Read and write I/Os are verified 		Verify the read/write IOs

Other testcases 


13.	create pages 100 with content "Page X" and perform 	Validates that the correct pages are read	correct pages should be read
	10000 random reads of these pages and check that the 							after creating pages and 
	correct pages are read											accessing random reads

14.	Try to pin page when pool is full of pinned pages	Returns error					Error expected

15.	Try to pin page with negative page number		Returns error					Error expected

16.	shutdown buffer pool that is not open			Returns error					Error expected

17.	pin page in buffer pool that is not open		Returns error					Error expected

18.	Unpin page which is not in the buffer pool		Returns error					Error expected

19.	Mark page dirty that is not in buffer pool		Returns error					Error expected

20. 	flush bufferpool that is not open			Returns error					Error expected

21.	try to init buffer pool for non existing page file	Returns error					Error expected


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~





