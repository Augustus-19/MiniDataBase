#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include <string.h>
#include <stdlib.h>



#define META_PAGENO 0 // first page reserved for metadata
#define COSNT_STRINGLEN 30 //string length we consider as constant length
#define BUFFER_FRAME_SIZE 10 // size of buffer frame to initialize buffer pool
#define PAGE_REPLACEMENT_ALGO RS_FIFO // which page replacement algo to use

// meta data to store in the first page
typedef struct MetaData {
	int numberOfTuples;
	int firstFreePage;
	int recordSize;
	int numberOfAttr;
	Schema schema;
}MetaData;

typedef struct RecordMgrData {
	int numberOfTuples;
	int firstFreePage;
	int recordSize;
	int numberOfAttr;
	Schema schema;
	BM_PageHandle bufferPageHandle;
	BM_BufferPool bufferPool;
}RecordMgrData;
// table and manager
RC initRecordManager(void *mgmtData) {
	initStorageManager();
	return RC_OK;
	
}

RC shutdownRecordManager() {
	return RC_OK;
}

void printRecordManager(RecordMgrData* recordMgr,Schema * schema,char *callingFunctionName) {


	int i;

	printf("\n*********%s********\n", callingFunctionName);
	printf("numberOfTuples = %d\n", recordMgr->numberOfTuples);
	printf("firstFreePage = %d\n", recordMgr->firstFreePage);
	printf("numberOfAttr = %d\n", recordMgr->numberOfAttr);
	printf("recordSize = %d\n", recordMgr->recordSize);
	
	for (i = 0; i < recordMgr->numberOfAttr; i++) {
		printf("attrNames %s ", schema->attrNames[i]);
		printf("DataType %d ", schema->dataTypes[i]);
		printf("DataLength %d \n", schema->typeLength[i]);
	}

	printf("key len %d\n", schema->keySize);
	for (i = 0; i < schema->keySize; i++) {
		printf("%d ->", schema->keyAttrs[i]);
	}

}

RC createTable(char *name, Schema *schema) {

	RC retVal = RC_OK;
	SM_FileHandle fh;
	//MetaData metaData;
	RecordMgrData metaData;
	int i = 0;
	int intSize = sizeof(int);
	
	retVal = createPageFile(name); // create pagefile for each table
	if (retVal != RC_OK) {
		printf("failed to create pageFile");
		return retVal;
	}

	// open pagefile to write metadata
	retVal = openPageFile(name, &fh);
	if (retVal != RC_OK) {
		printf("failed to open pageFile");
		return retVal;
	}

	metaData.numberOfTuples = 0;
	metaData.firstFreePage = 1;
	metaData.recordSize = getRecordSize(schema);
	metaData.numberOfAttr = schema->numAttr;
	// copying all schema relate info to metadata
	metaData.schema.attrNames = schema->attrNames;
	metaData.schema.dataTypes = schema->dataTypes;
	metaData.schema.keyAttrs = schema->keyAttrs;
	metaData.schema.keySize = schema->keySize;
	metaData.schema.numAttr = schema->numAttr;
	metaData.schema.typeLength = schema->typeLength;

	//to write first page of file with metadata
	char page[PAGE_SIZE];
	char *pagePtr = (char *)&page;
	// testing
	char readPage[PAGE_SIZE];
	char *readPagePtr = readPage;
	memset(readPagePtr, 0, PAGE_SIZE);
	memset(pagePtr, 0, PAGE_SIZE);

	*((int*)pagePtr) = metaData.numberOfTuples;
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.firstFreePage;
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.recordSize;
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.numberOfAttr;
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	// copy all attributes detail
	for (i = 0; i < schema->numAttr; i++) {

		strncpy(pagePtr, schema->attrNames[i], COSNT_STRINGLEN);
		pagePtr = pagePtr + COSNT_STRINGLEN;

		*(int*)pagePtr = (int)schema->dataTypes[i];
		pagePtr = pagePtr + intSize;

		*(int*)pagePtr = (int)schema->typeLength[i];
		pagePtr = pagePtr + intSize;

	}

	*(int*)pagePtr = (int)schema->keySize;
	pagePtr = pagePtr + intSize;

	//copy all key details
	for (i = 0; i < schema->keySize; i++) {
		*(int*)pagePtr = schema->keyAttrs[i];
		pagePtr = pagePtr + intSize;
	}

	retVal = writeBlock(META_PAGENO, &fh, page);
	if (retVal != RC_OK) {
		printf("writing block failed");
		return retVal;
	}

	/* for testig */
	//retVal = readBlock(META_PAGENO, &fh, pagePtr);
	
	retVal = closePageFile(&fh);
	if (retVal != RC_OK) {
		printf("close pageFile failed");
		return retVal;
	}
	printRecordManager(&metaData, schema, "createFun");
	
	return retVal;
}

RC openTable(RM_TableData *rel, char *name) {

	RC retVal;
	SM_PageHandle pagePtr;
	RecordMgrData* recordMgrData = (RecordMgrData*)malloc(sizeof(RecordMgrData));
	Schema *schema = (Schema*)malloc(sizeof(Schema));
	int i = 0;
	int intSize = sizeof(int);


	rel->mgmtData = recordMgrData;
	rel->name = name;

	retVal = initBufferPool(&recordMgrData->bufferPool, name, BUFFER_FRAME_SIZE, PAGE_REPLACEMENT_ALGO, NULL);
	if (retVal != RC_OK) {
		printf("BufferPool Initialization failed");
		return retVal;
	}

	retVal = pinPage(&recordMgrData->bufferPool, &recordMgrData->bufferPageHandle, META_PAGENO);
	if (retVal != RC_OK) {
		printf("PinPage failed");
		return retVal;
	}

	pagePtr = (char*)recordMgrData->bufferPageHandle.data;

	recordMgrData->numberOfTuples = *((int*)pagePtr);
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	recordMgrData->firstFreePage = *((int*)pagePtr);
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	recordMgrData->recordSize = *((int*)pagePtr);
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	recordMgrData->numberOfAttr = *((int*)pagePtr);
	schema->numAttr = *((int*)pagePtr);
	//printf("%d", *((int*)pagePtr));
	pagePtr = pagePtr + intSize;

	// based on attrib num allocate memory for details of attrib
	schema->attrNames = (char**)malloc((sizeof(char*))*(schema->numAttr));
	schema->dataTypes = (DataType*)malloc(sizeof(DataType)*(schema->numAttr));
	schema->typeLength = (int*)malloc(sizeof(int)*schema->numAttr);
	
	int *check = (int*)malloc(sizeof(int)*100);

	//allocate memory for name
	for (i = 0; i < schema->numAttr; i++) {
		schema->attrNames[i] = (char*)malloc(COSNT_STRINGLEN);
	}

	// copy back all attributes detail
	for (i = 0; i < schema->numAttr; i++) {

		strncpy( schema->attrNames[i], pagePtr, COSNT_STRINGLEN);
		pagePtr = pagePtr + COSNT_STRINGLEN;

		schema->dataTypes[i] = (*(int*)pagePtr);
		pagePtr = pagePtr + intSize;

		schema->typeLength[i] = (*(int*)pagePtr);
		pagePtr = pagePtr + intSize;

	}

	schema->keySize = *(int*)pagePtr;
	pagePtr = pagePtr + intSize;

	schema->keyAttrs = (int*)malloc(((sizeof(int))*(schema->keySize)));
	//copy all key details
	for (i = 0; i < schema->keySize; i++) {
		schema->keyAttrs[i] = *(int*)pagePtr;
		pagePtr = pagePtr + intSize;
	}

	rel->schema = schema;

	retVal = unpinPage(&recordMgrData->bufferPool, &recordMgrData->bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin failed ");
		return retVal;
	}
	printRecordManager(recordMgrData, schema, "OpenTable");
	return retVal;
}

RC closeTable(RM_TableData *rel) {
	
	RC retVal = RC_OK;
	SM_FileHandle fh;
	RecordMgrData* recordMgrData = (RecordMgrData*)malloc(sizeof(RecordMgrData));
	Schema *schema = (Schema*)malloc(sizeof(Schema));

}

RC deleteTable(char *name) {

}

int getNumTuples(RM_TableData *rel) {

}


// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {

}

RC deleteRecord(RM_TableData *rel, RID id) {

}

RC updateRecord(RM_TableData *rel, Record *record) {

}

RC getRecord(RM_TableData *rel, RID id, Record *record) {

}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {

}

RC next(RM_ScanHandle *scan, Record *record) {

}

RC closeScan(RM_ScanHandle *scan) {

}


// dealing with schemas
int getRecordSize(Schema *schema) {

	int recordSize = 0;
	int i;
	for (i = 0; i < schema->numAttr; ++i) {
		switch (schema->dataTypes[i]) {
		case DT_BOOL:
			recordSize = recordSize + sizeof(bool);
			break;
		case DT_FLOAT:
			recordSize = recordSize + sizeof(float);
			break;
		case DT_INT:
			recordSize = recordSize + sizeof(int);
			break;
		case DT_STRING:
			recordSize = recordSize + schema->typeLength[i];
			break;
		default:
			printf("shouldnt be here!!");
			break;
		}
	}

	return recordSize;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {

	Schema *schema = malloc(sizeof(Schema));
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->keyAttrs = keys;
	schema->keySize = keySize;
	schema->numAttr = numAttr;
	schema->typeLength = typeLength;
	return schema;
}

RC freeSchema(Schema *schema) {

}


// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {

}

RC freeRecord(Record *record) {

}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {

}
