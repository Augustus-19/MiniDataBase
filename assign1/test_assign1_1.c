#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"
/* test output file2 */
#define TESTPF2 "test_pagefile2.bin"
#define TESTPF3 "duplicateTest.bin"



/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testOpenEachPage_checkValues(void);
static void testTryToOpenWrongFile(void);
static void testAccessPageOutOfRange(void);
static void testPassNULL(void);
static void testEnsureCapacity(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  /*new test cases */ 
  testTryToOpenWrongFile();  
  testOpenEachPage_checkValues();
  testAccessPageOutOfRange();
  testPassNULL();
  testEnsureCapacity();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  printf("first\n");
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}



/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  
  TEST_DONE();
}

/* try to open wrong file test */
void 
testTryToOpenWrongFile(void){

  SM_FileHandle fh;
  
  testName = "try to open wrong file";

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  /* try to open wrong file*/
  TEST_CHECK_OPENWITHWRONGFIEL(openPageFile (TESTPF2, &fh));

  /* try to delete wrong file */
  TEST_CHECK_DELETEWITHWRONGFIEL(destroyPageFile (TESTPF2));

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));
  TEST_DONE();


}

/* Try to read all blocks and cross check the values */
void
testOpenEachPage_checkValues(){

  SM_FileHandle fh;
  SM_PageHandle ph;
  int i, j;
  int dataToWrite,dataToRead;

  testName = "Test each page content";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");

  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
    
  // read first page into handle to reset the postion to 0
  TEST_CHECK(readFirstBlock (&fh, ph));


  for(j = 0;j < 10; j++){

    // write content of page as position of the page
    dataToWrite = getBlockPos(&fh);

    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
      ph[i] = dataToWrite;
    TEST_CHECK(writeBlock (dataToWrite, &fh, ph));
    TEST_CHECK(appendEmptyBlock(&fh));

  }

  for(j = 0;j < 10; j++){
    if(j == 0){
       TEST_CHECK(readFirstBlock (&fh, ph));
    }else{
       TEST_CHECK(readNextBlock (&fh, ph));
    }

    // read content of page as position of the page
    dataToRead = getBlockPos(&fh);
    printf("dataToRead is : %d\n",dataToRead);    

    // change ph to be a string and write that one to disk
    for (i=0; i < PAGE_SIZE; i++)
      ASSERT_TRUE((ph[i] == dataToRead), "character in page read from disk is the one we expected.");
   
  }
  
  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  
  TEST_DONE();


}


/* Try to seek to position ourside the file page range  */
void
testAccessPageOutOfRange(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;

  testName = "test accessing page which is out of range";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  /* try to read next page which is not presetn */

  TEST_CHECK_OUTOFRANGE(readNextBlock (&fh, ph));
  TEST_CHECK_OUTOFRANGE(readPreviousBlock (&fh, ph));

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  TEST_DONE();
}

/* Test with NULL parameters */
void
testPassNULL(void)
{

  SM_PageHandle ph;

  testName = "test for invalid parameters";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  // read first page into handle
  TEST_CEHCK_NULL(readFirstBlock (NULL, ph));
  TEST_CEHCK_NULL(createPageFile (NULL));
  TEST_CEHCK_NULL(openPageFile (NULL,NULL));
  TEST_CEHCK_NULL(closePageFile (NULL));
  TEST_CEHCK_NULL(destroyPageFile (NULL));

  TEST_DONE();
}


/* test ensure capacity by giving page size and checking if page exists */
void
testEnsureCapacity(void)
{

  
  SM_FileHandle fh;
  SM_PageHandle ph;  
  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  testName = "test to check ensure capacity";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");


  TEST_CHECK(ensureCapacity(20,&fh));
  TEST_CHECK(readLastBlock(&fh,ph));
  ASSERT_TRUE(getBlockPos(&fh) == 19, "got expected value 19");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));
  
  TEST_DONE();

}

/*
void
testToRecreateExistingFile(void)
{

  testName = "test to check duplicate file creation";

  TEST_DUPLICATE_CREATION(createPageFile (TESTPF3));
  
  TEST_DONE();
}
*/




