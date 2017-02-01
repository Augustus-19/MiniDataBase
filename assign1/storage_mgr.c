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

void initStorageManager (void)
{
	io_buffer = (char *) malloc (PAGE_SIZE);
}

RC createPageFile (char *fileName)
{

	int fd;
	int ret;

	/* invalid or null pointer check*/	
	if(fileName == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	// Create new file 
	if ( (fd = open(fileName, O_WRONLY | O_CREAT | O_SYNC, FILE_MODE)) < 0) 
	{
		perror("Error creating page file");
		return RC_FILE_NOT_FOUND;
	}

	// Add one page to newly created file
	memset((void *) io_buffer, 0, PAGE_SIZE);

	if ( (ret = write(fd, (void *) io_buffer, PAGE_SIZE)) < 0 ) 
	{	
		perror("Error writing to file");
    		return RC_FILE_NOT_FOUND;
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

	/* invalid or null pointer check*/	
	if(fileName == NULL || fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	if ((fd = open(fileName, O_RDWR, FILE_MODE)) < 0)
	{
		perror("Error opening page file");
		return RC_FILE_NOT_FOUND;
	}

	if ((ret = fstat(fd, &st)) < 0)
	{
		perror("Error obtaining file stats");
		return RC_FILE_NOT_FOUND;
	}

	len_file = st.st_size;
	
	fHandle->fileName = fileName;
	fHandle->totalNumPages = len_file / PAGE_SIZE;
	fHandle->curPagePos = 0;
  	fHandle->mgmtInfo = malloc(sizeof(Mgmt_Info));
  	((Mgmt_Info*)fHandle->mgmtInfo)->fd = fd;

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
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
			if((msync(addr,len,MS_SYNC)) < 0)
			perror("Error in msync");

	    	if(munmap(addr,len) == -1)
			perror("Error in munmap");
    }
	
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
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		void *addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		// read page from => startOfMemory + (pageNumber * pageSize)
		memcpy(memPage, addr + (PAGE_SIZE * pageNum), PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(lseek(fd, (PAGE_SIZE * pageNum), SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
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
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		void *addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		memcpy(memPage, addr, PAGE_SIZE);
	}
	else
	{
  		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
  		if(lseek(fd, 0, SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	
	fHandle->curPagePos = 0;
	return RC_OK;
}


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}

	/* avoid reading page number less than zero*/
	if((fHandle->curPagePos - 1) < 0){
		perror("no previous block");
		return RC_PAGE_OUTOFBOUND;
	
	}
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon - 1) * pageSize
		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->curPagePos - 1)*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(lseek(fd, -PAGE_SIZE, SEEK_CUR) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	
	fHandle->curPagePos--;

	return RC_OK;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;
	}
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon * pageSize)
		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->curPagePos)*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	//fHandle->curPagePos = 1;  // no need to update
	return RC_OK;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

	/* avoid reading page number less than zero*/
	if((fHandle->curPagePos + 1) >= fHandle->totalNumPages){
		perror("no next block");
		return RC_PAGE_OUTOFBOUND;
	
	}

	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon + 1) * pageSize
		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->curPagePos + 1)*PAGE_SIZE);
		memcpy(memPage,addr,PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(lseek(fd, +PAGE_SIZE, SEEK_CUR) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	fHandle->curPagePos++;
	return RC_OK;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (totalNumPages - 1) * pageSize
		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->totalNumPages - 1) * PAGE_SIZE);
		memcpy(memPage, addr, PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(read(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error reading page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	fHandle->curPagePos = fHandle->totalNumPages - 1;
	return RC_OK;

}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
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
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{

		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr + (PAGE_SIZE*pageNum));
		// read page from => startOfMemory + ( pageSize * pageNumber)
		memcpy(addr, memPage, PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(lseek(fd, (PAGE_SIZE * pageNum), SEEK_SET) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}

 		if(write(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
	fHandle->curPagePos = pageNum;
	return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}
	
	BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE)
	{
		// starting address + (currentPagePositon * pageSize)
		void *addr = (((Mgmt_Info*)fHandle->mgmtInfo)->map_addr)+((fHandle->curPagePos)*PAGE_SIZE);
		memcpy(addr, memPage, PAGE_SIZE);
	}
	else
	{
		int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
		if(write(fd, (void *) memPage, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
        /* no need to update position */
	return RC_OK;
}

RC appendEmptyBlock (SM_FileHandle *fHandle)
{

	/* invalid or null pointer check*/	
	if(fHandle == NULL){
		perror("Invalid parameter");
		return RC_NULL_PARAM;

	}

        int new_len = (fHandle->totalNumPages * PAGE_SIZE) + PAGE_SIZE;
        int fd = ((Mgmt_Info*)fHandle->mgmtInfo)->fd;
        
        if (ftruncate(fd, new_len) != 0)
        {
            perror("Error extending file");
            return RC_FILE_NOT_EXTENSIBLE;
        }
        
        BOOL mmapped = ((Mgmt_Info*)fHandle->mgmtInfo)->mmapped;
	
	if(mmapped == TRUE) 
	{
		int len = ((Mgmt_Info*)fHandle->mgmtInfo)->map_size;
		char * addr = ((Mgmt_Info*)fHandle->mgmtInfo)->map_addr;
		
		if ((addr = mremap(addr, len, new_len, MREMAP_MAYMOVE)) == MAP_FAILED)
		{
		    	perror("Error extending mapping");
		    	((Mgmt_Info*)fHandle->mgmtInfo)->mmapped = FALSE;
		    	

			if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
			{
				perror("Error seeking to page in file");
				return RC_PAGE_OUTOFBOUND;
			}
			
			memset((void *) io_buffer, 0, PAGE_SIZE);
			
	 		if(write(fd, (void *) io_buffer, PAGE_SIZE) == -1)
	 		{
	 			perror("Error writing page in file");
				return RC_PAGE_OUTOFBOUND;
			}
		}
		else
		{
			((Mgmt_Info*)fHandle->mgmtInfo)->map_addr = addr;
        		((Mgmt_Info*)fHandle->mgmtInfo)->map_size = new_len;
        		memset(addr+len, '0', PAGE_SIZE);
		}
        }
        else
        {
        	if(lseek(fd, -PAGE_SIZE, SEEK_END) == -1)
		{
			perror("Error seeking to page in file");
			return RC_PAGE_OUTOFBOUND;
		}
		
		memset((void *) io_buffer, 0, PAGE_SIZE);
		
 		if(write(fd, (void *) io_buffer, PAGE_SIZE) == -1)
 		{
 			perror("Error writing page in file");
			return RC_PAGE_OUTOFBOUND;
		}
	}
        

        fHandle->totalNumPages += 1;
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

	while(fHandle->totalNumPages < numberOfPages)
    		appendEmptyBlock(fHandle);
  	return RC_OK;
}
