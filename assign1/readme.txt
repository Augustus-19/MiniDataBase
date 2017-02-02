ASSIGNMENT 1:

OBJECTIVE: To implement a simple storage manager - a module that is capable of reading blocks from a file on disk into memory and writing blocks from memory to a file on disk


TEAM MEMBERS:
1. Kiran C D
2. Sashank B V
3. Adarsh S

FILES INCLUDED:

1.a.out
2.dberror.c
3.dberror.h
4.Makefile
5.output.txt
6.readme.txt
7.storage_mgr.c
8.storage_mgr.h
9.test_assign1_1.c
10.test_helper.h

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
HOW TO BUILD THE PROJECT:
	1) run "make all" from the assign1 directory, this will create output file a.out
	2) run "make clean" from the assign1 directory to delete all files created by previous build


HOW TO RUN THE PROJECT:
	1) Build the project using description in previous section
	2) run "./a.out" to run the testcases defined in test_assign1_1.c

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FUNCTION DESCRIPTION:


SL NO 		NAME				DESCRIPTION
-----------------------------------------------------------------------------------------------------------------------------------------------
1.		createPageFile			Creating a new page and initializing the first block to be zero

2.		openPageFile			Opens an existing file and if the file doesnot exist, throws an error
 
3.		closePageFile			Closes an existing page file which is opened before

4.		destroyPageFile			Destroying an existing file which was created before
		
5.		readBlock			Reads a block from the file and stores it in memory page.If the block does not exist 							;returns error

6.		getBlockPos			Return the current page position in a file 

7.		readFirstBlock			Reads the first page in the file

8.		readPreviousBlock		Reads the previous block relative to the current page position in the file. If the previous 							block does not exist; returns error

9.		readCurrentBlock		Reads the block at the current page position in the file. If the current block does not exist; 							returns error

10.		readNextBlock			Reads the next block relative to the current page position in the file. If the next block 							does not exist; returns error

11.		readLastBlock			Reads the last page in the file

12.		writeBlock			Writes the given page data to a particular page position in the file
		
13.		writeCurrentBlock		Writes the given page data to current page position of the file	
		
14.		appendEmptyBlock		Increase the number of pages in the file by one

15.		ensureCapacity			Ensures the file contains the given number pages and appends more if it does not




~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DESIGN DESCRIPTION & FUTURE WORK:

In our implementation of storage manager we map page file to memory whenever it is possible to do so. 
Memory mapping page file improves I/O performance of the storage manager and overall performance of the system which will be built upon this. 
Also from the storage manager's perspective it becomes natural way of handling any disk I/O instead of performing the file read/write operations. 
One drawback of memory mapping files is that it might not be always possible to memory map a file based on the current memory usage by the 
system. 
This drawback can be handled by either by mapping part of file and remapping whenever necessary or rollback to conventional file 
read/write.
In our implementation we have rollback mechanism to use conventional file read/write whenever file mapping fails.
Complete memory mapped handling of page file is future work which involves much more intelligent way of mapping and remapping parts of file 
to improve performance.


~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TESTCASES:

SL NO		Objective						Observed result			Expected result
---------------------------------------------------------------------------------------------------------------------------------------------
1.		Destroying an existing file and trying			Throws an error			Error expected
		to open the same should throw an
		error

2.		Reading the first block in a newly initialized		Zero byte observed		Zero byte expected
		page and expecting it to be zero

3.		Write a string to the first block/page of the file	First block is written		First block should be filled with 														given data 		

4.		Verify the previously written page in file by reading	Read data matches with the 	Data read should match with the 
									previously written data		previously written data	
		
5.		Destroying the previously created file 			File is destroyed 		File should be destroyed

6.		Trying to open a file which does not exist 		Throws an error			Error expected

7.		Trying to destroy a file which does not exist 		Throws an error			Error expected

8.		Write to multiple blocks one after the other to a 	Multiple blocks are written	Multiple blocks should be written
		a newly created file
		
9.		Try to read previously written file with all variants	Blocks are read correctly based Blocks should be read correctly based
		of read API						on their position		on their position


10.		Trying to create a new file with the file name		Throws an error			Error expected
		containing invalid characters

11.		Trying to open a file which exists			File is opened sucessfully	File should be opened sucessfully

12.		Trying to open a file which is already opened		Throws an error			Error expected

13. 		Trying to close a file which does not exist		Throws an error			Error expected

14.		Trying to close a file which is already closed 		Throws an error			Error expected

15.		Trying to read a block beyond the page size 		Throws an error 		Error Expected

16.		Trying to write a block beyond the page size		Throws an error			Error Expected

17.		Verifying if the current page position is updated	Page position is updated	Page position should be updated
		with each read/write operation				correctly			correctly

18.		Try to append a empty page to a open file & verify	Appends successfully		Page should be appended successfully

19.		Verify ensure capacity by reading block at the end	Ensure capacity successfully 	Ensure capacity should be successfully 			of the file						verified			verified			
												

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			
