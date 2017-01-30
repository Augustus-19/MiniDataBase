#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

#include "storage_mgr.h"

void initStorageManager (void)
{

}

RC createPageFile (char *fileName)
{
	int fd;

	fd = open(fileName, O_WRONLY | O_CREAT | O_SYNC, 0644);
	if(fd < 0) 
	{
		perror("Error creating page file");
		return RC_FILE_NOT_FOUND;
	}
	
	return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle)
{
	return RC_OK;
}
RC destroyPageFile (char *fileName)
{
	return RC_OK;
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
int getBlockPos (SM_FileHandle *fHandle)
{
	return 0;
}
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
	return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	return RC_OK;
}
