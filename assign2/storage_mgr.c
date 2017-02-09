#define _GNU_SOURCE
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"

#define FILE_MODE S_IRWXU | S_IRGRP | S_IROTH

static char * io_buffer = NULL;
const bool FORCE_DISK_IO = TRUE; //To force using file I/O instead of memory mapping

void initStorageManager (void)
{
	io_buffer = (char *) malloc (PAGE_SIZE);
}

RC createPageFile (char *fileName)
{

	int fd;
	int ret;

	/* Invalid or null pointer check*/	
	if(fileName == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	// Create new file 
	if ( (fd = open(fileName, O_WRONLY | O_CREAT | O_SYNC , FILE_MODE)) < 0) 
	{
		perror("Error creating page file");
		return RC_FILE_NOT_FOUND;
	}

	// Add one page to newly created file
	memset((void *) io_buffer, 0, PAGE_SIZE);

	if ( (ret = write(fd, (void *) io_buffer, PAGE_SIZE)) < 0 ) 
	{	
		perror("Error writing to file");
    		return RC_FILE_SEEK_OR_IO_FAIL;
  	}
	// Close file
	sync();
	close(fd);
	
	return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	struct stat st;
	int fd;
	int ret;
	size_t len_file;
	char* addr = NULL;

	/* Invalid or null pointer check*/	
	if(fileName == NULL || fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	//Open file to read/write
	if ((fd = open(fileName, O_RDWR, FILE_MODE)) < 0)
	{
		perror("Error opening page file");
		return RC_FILE_NOT_FOUND;
	}

	//Get file stat
	if ((ret = fstat(fd, &st)) < 0)
	{
		perror("Error obtaining file stats");
		return RC_FILE_NOT_FOUND;
	}

	len_file = st.st_size;
	
	fHandle->fileName = fileName;
	fHandle->totalNumPages = len_file / PAGE_SIZE; //Since file grows in PAGE_SIZE units
	fHandle->curPagePos = 0;
  	fHandle->mgmtInfo = malloc(sizeof(Mgmt_Info));
  	((Mgmt_Info*)fHandle->mgmtInfo)->fd = fd;
	
	//Check if forced to used disk io
	if(FORCE_DISK_IO == FALSE) 
	{		
		//Memory map file		
		if ((addr = mmap(NULL, len_file, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED)
		{
			((Mgmt_Info*)fHandle->mgmtInfo)->mmapped = FALSE;
			perror("Error memory mapping file");
		}
		else
		{
			((Mgmt_Info*)fHandle->mgmtInfo)->mmapped = TRUE;
			((Mgmt_Info*)fHandle->mgmtInfo)->map_addr = addr;	
	  		((Mgmt_Info*)fHandle->mgmtInfo)->map_size = len_file;
		}
	}
	else 
	{
		((Mgmt_Info*)fHandle->mgmtInfo)->mmapped = FALSE;
	}
	
	return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	char* addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
	size_t len = ((Mgmt_Info*)fHandle->mgmtInfo)->map_size;
	int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	// check if file is memory mapped
	if(mmapped == TRUE)
	{
		//Sync Inmemory changes		
		if((msync(addr,len,MS_SYNC)) < 0)
			perror("Error in msync");

		//Unmap file from memory	    	
		if(munmap(addr,len) == -1)
			perror("Error in munmap");
    	}
	
	//Close open file
	close(fd);
	
	free(fHandle->mgmtInfo);
	return RC_OK;
}

RC destroyPageFile (char *fileName)
{
	int ret;

	/* invalid or null pointer check*/	
	if(fileName == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	//Delete file from current directory
	if((ret = unlink(fileName)) < 0){
  		perror("File not destroyed");
		return RC_DELETE_FAILED;

	}

  	return RC_OK;
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}

	/*to handle page out of bound*/
	if((pageNum >= fHandle->totalNumPages) || (pageNum < 0)){
		perror("pageNum is out of bound");
		return RC_PAGE_OUTOFBOUND;
	}

	// check if file is memory mapped
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		void *addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		// read page from => startOfMemory + (pageNumber * pageSize)
		memcpy(memPage, addr + (PAGE_SIZE * pageNum), PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		
		//Seek to pageNum page
		if(lseek(fd, (PAGE_SIZE * pageNum), SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}
		
		//Read PAGE_SIZE data from file
 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	
	//Set current page position to pageNum
	fHandle->curPagePos = pageNum; 
	return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle)
{
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}

	return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}
	
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		void *addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		memcpy(memPage, addr, PAGE_SIZE);
	}
	else
	{
  		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		
		//Seek to first page  		
		if(lseek(fd, 0, SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
		
		//Read PAGE_SIZE data from file
 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
	
	fHandle->curPagePos = 0;
	return RC_OK;
}


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	void *addr;
	int fd, PrevPagePos;

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	/* avoid reading page number less than zero*/
	PrevPagePos = fHandle->curPagePos - 1;
	
	/* avoid reading page number less than zero*/
	if(PrevPagePos < 0 || PrevPagePos >= fHandle->totalNumPages ){
		perror("no previous block");
		return RC_PAGE_OUTOFBOUND;
	
	}
	
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon - 1) * pageSize
		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+(PrevPagePos*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		
		//Seek to previous page 		
		if(lseek(fd, PrevPagePos * PAGE_SIZE, SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
		
		//Read PAGE_SIZE data from file
 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
	
	fHandle->curPagePos--;

	return RC_OK;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	void *addr;
	int fd, curPage;

	curPage = fHandle->curPagePos;
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}

	/* to handle page out of bound*/
	if((curPage >= fHandle->totalNumPages) || (curPage < 0)){
		perror("!!! Should Not Enter this ");
		return RC_PAGE_OUTOFBOUND;
	}	

	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon * pageSize)
		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+(curPage*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;

		// seek to current page position
		if(lseek(fd, curPage*PAGE_SIZE, SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}

		// read PAGE_SIZE data from file
		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
	
	return RC_OK;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	void *addr;
	int fd, nextPagePos;

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	/* avoid reading page number less than zero*/
	nextPagePos = fHandle->curPagePos + 1;
	if(nextPagePos >= fHandle->totalNumPages){
		perror("no next block");
		return RC_PAGE_OUTOFBOUND;
	}

	// check if file is memory mapped
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon + 1) * pageSize
		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+(nextPagePos*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		// seek to nextpage
		if(lseek(fd, nextPagePos*PAGE_SIZE, SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}

		//read a page from seeked position
 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
	fHandle->curPagePos++;
	return RC_OK;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	void *addr;
	int fd;

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (totalNumPages - 1) * pageSize
		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->totalNumPages - 1) * PAGE_SIZE);
		memcpy(memPage, addr, PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		// seek PAGE_SIZE back from the end of the file
		if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}

		//read the last page of the file
 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
	fHandle->curPagePos = fHandle->totalNumPages - 1;
	return RC_OK;

}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	void *addr;
	int fd;

	pageNum = fHandle->curPagePos;
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}

	/*to handle page out of bound*/
	if((pageNum >= fHandle->totalNumPages) || (pageNum < 0)){
		perror("pageNum is out of bound");
		return RC_PAGE_OUTOFBOUND;
	}
	
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{

		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr + (PAGE_SIZE*pageNum));
		// read page from => startOfMemory + ( pageSize * pageNumber)
		memcpy(addr, memPage, PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		// seek to pageNum
		if(lseek(fd, (PAGE_SIZE * pageNum), SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}

		// write memPage content to seeked position
 		if(write(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}

	// update current position 
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	void *addr;
	int fd, pageNum;


	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	pageNum = fHandle->curPagePos;
	/* to handle page out of bound*/
	if((pageNum >= fHandle->totalNumPages) || (pageNum < 0)){
		perror("!!! Should Not Enter this ");
		return RC_PAGE_OUTOFBOUND;
	}
	
	bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon * pageSize)
		addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+(pageNum*PAGE_SIZE);
		memcpy(addr, memPage, PAGE_SIZE);
	}
	else
	{
		fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		pageNum = fHandle->curPagePos;
		// seek to current page position
		if(lseek(fd, (PAGE_SIZE * pageNum), SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}

		// write data to seeked position
		if(write(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
        /* no need to update position */
	return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{

	char * addr;
	int len;


	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

        int new_len = (fHandle->totalNumPages * PAGE_SIZE) + PAGE_SIZE;
        int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
        
	//Increase file size on disk by PAGE_SIZE bytes
        if (ftruncate(fd, new_len) != 0)
        {
            perror("Error extending file");
            return RC_FILE_NOT_EXTENSIBLE;
        }
        
        bool mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE) 
	{
		len = ((Mgmt_Info*)fHandle->mgmtInfo)->map_size;
		addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		
		//File has to be remapped to new size
		if ((addr = mremap(addr, len, new_len, MREMAP_MAYMOVE)) == MAP_FAILED) //MREMAP_MAYMOVE - may remap the file to different address range
		{
		    	perror("Error extending mapping");
		    	
			//If remap fails fall back to reading/writing file on disk
			((Mgmt_Info*)fHandle->mgmtInfo)->mmapped = FALSE;
		    	
			//Seek to newly appended page
			if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
			{
				perror("Error seeking to page in file");
				return RC_FILE_SEEK_OR_IO_FAIL;
			}
			
			memset((void *) io_buffer, 0, PAGE_SIZE);
			
			//Write Zero to newly appended page
	 		if(write(fd, (void *) io_buffer, PAGE_SIZE) == -1)
	 		{
	 			perror("Error writing page in file");
				return RC_FILE_SEEK_OR_IO_FAIL;
			}
		}
		else
		{
			((Mgmt_Info*)fHandle->mgmtInfo)->map_addr = addr;
        		((Mgmt_Info*)fHandle->mgmtInfo)->map_size = new_len;
			
			//Write Zero to newly appended page        		
			memset(addr+len, '0', PAGE_SIZE);
		}
        }
        else
        {
		//Seek to newly appended page
        	if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
		{
			perror("Error seeking to page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
		
		memset((void *) io_buffer, 0, PAGE_SIZE);
		
		//Write Zero to newly appended page
 		if(write(fd, (void *) io_buffer, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_FILE_SEEK_OR_IO_FAIL;
		}
	}
        
	//Increase total page count by 1
        fHandle->totalNumPages += 1;

	//Update current page position to last appended page
        fHandle->curPagePos = fHandle->totalNumPages - 1;
        
	return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	//Append more pages to ensure capacity
	while(fHandle->totalNumPages < numberOfPages)
    		appendEmptyBlock(fHandle);
  	return RC_OK;
}
