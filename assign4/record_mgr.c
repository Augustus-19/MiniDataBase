#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include <string.h>
#include <stdlib.h>
#include <math.h>

#define META_PAGENO 0 // Page reserved for metadata
#define MAX_ATTRNAME_LEN 30 //Maximum length of the attribute name
#define BUFFER_FRAME_SIZE 10 // Size of page frame to initialize buffer pool
#define PAGE_REPLACEMENT_ALGO RS_FIFO // Page replacement algorithm to use
#define RECORD_DELIMITER '#'
#define RECORD_DELIMITER_TOMB '~'
#define TOMBSTONE 0

// Table meta data
typedef struct RecordMgrData
{
	int numberOfTuples;
	int firstFreePage;
	int recordSize;
	int numberOfAttr;
	Schema schema;
	BM_BufferPool bufferPool;
}RecordMgrData;

typedef struct RM_ScaningRecordInfo
{
	Expr *condition; //Search condition
	int pageCurrentPosition; //Current page index
	int slotCurrentPosition; // Current slot index
}RM_ScaningRecordInfo;

// table and manager
RC initRecordManager(void *mgmtData)
{
	RC retVal = RC_OK;
	initStorageManager();
	return retVal;
}

RC shutdownRecordManager()
{
	RC retVal = RC_OK;
	return retVal;
}

void printRecordManager(RecordMgrData* recordMgr, Schema * schema, char *callingFunctionName)
{
	int i;

	printf("\n*********%s********\n", callingFunctionName);
	printf("Number of Tuples = %d\n", recordMgr->numberOfTuples);
	printf("First Free Page = %d\n", recordMgr->firstFreePage);
	printf("Number of Attributes = %d\n", recordMgr->numberOfAttr);
	printf("Record Size = %d\n", recordMgr->recordSize);
	
	for (i = 0; i < recordMgr->numberOfAttr; i++) {
		printf("AttributeName %s ", schema->attrNames[i]);
		printf("DataType %d ", schema->dataTypes[i]);
		printf("DataLength %d \n", schema->typeLength[i]);
	}

	printf("Number of Key Attributes = %d\nKey Attributes = ", schema->keySize);
	for (i = 0; i < schema->keySize; i++) {
		printf("%d, ", schema->keyAttrs[i]);
	}
    printf("\b\b\n");
}

RC createTable(char *name, Schema *schema) {

	RC retVal = RC_OK;
	SM_FileHandle fh;
	RecordMgrData metaData;
	int i = 0;
	int intSize = sizeof(int);
	
	//Create pagefile for each table
	retVal = createPageFile(name);
	if (retVal != RC_OK)
	{
		printf("Failed to create pageFile");
		goto END;
	}

	//Open pagefile to write metadata
	retVal = openPageFile(name, &fh);
	if (retVal != RC_OK)
	{
		printf("Failed to open pageFile");
		goto END;
	}

	//Initialize
	metaData.numberOfTuples = 0;
	metaData.firstFreePage = 1;
	metaData.recordSize = getRecordSize(schema);
	metaData.numberOfAttr = schema->numAttr;

	//Copy all schema related data from schema structure
	metaData.schema.attrNames = schema->attrNames;
	metaData.schema.dataTypes = schema->dataTypes;
	metaData.schema.keyAttrs = schema->keyAttrs;
	metaData.schema.keySize = schema->keySize;
	metaData.schema.numAttr = schema->numAttr;
	metaData.schema.typeLength = schema->typeLength;

	//For writing first page of pagefile with table metadata
	char page[PAGE_SIZE];
	char *pagePtr = page;

	//For validating the write
	//char readPage[PAGE_SIZE];
	//char *readPagePtr = readPage;
	//memset(readPagePtr, 0, PAGE_SIZE);

	memset(pagePtr, 0, PAGE_SIZE);

	*((int*)pagePtr) = metaData.numberOfTuples;
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.firstFreePage;
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.recordSize;
	pagePtr = pagePtr + intSize;

	*((int*)pagePtr) = metaData.numberOfAttr;
	pagePtr = pagePtr + intSize;

	// Copy all attribute's detail
	for (i = 0; i < schema->numAttr; i++) {

		strncpy(pagePtr, schema->attrNames[i], MAX_ATTRNAME_LEN);
		pagePtr = pagePtr + MAX_ATTRNAME_LEN;

		*(int*)pagePtr = (int)schema->dataTypes[i];
		pagePtr = pagePtr + intSize;

		*(int*)pagePtr = (int)schema->typeLength[i];
		pagePtr = pagePtr + intSize;

	}

	*(int*)pagePtr = (int)schema->keySize;
	pagePtr = pagePtr + intSize;

	//Copy all key details
	for (i = 0; i < schema->keySize; i++) {
		*(int*)pagePtr = schema->keyAttrs[i];
		pagePtr = pagePtr + intSize;
	}

	retVal = writeBlock(META_PAGENO, &fh, page);
	if (retVal != RC_OK) {
		printf("Writing block failed\n");
		goto END;
	}

	//For validating the write
	//retVal = readBlock(META_PAGENO, &fh, readPagePtr);
	//if (retVal != RC_OK)
	//{
		//printf("Reading block failed\n");
		//goto END;
	//}
	
	retVal = closePageFile(&fh);
	if (retVal != RC_OK) {
		printf("close pageFile failed");
		goto END;
	}

	/* enable below function to cross verify data */
	 printRecordManager(&metaData, schema, "createTable");

END:
	return retVal;
}

RC openTable(RM_TableData *rel, char *name)
{
	RC retVal = RC_OK;
	SM_PageHandle pagePtr;
	RecordMgrData* recordMgrData = (RecordMgrData*)malloc(sizeof(RecordMgrData));
	Schema *schema = (Schema*)malloc(sizeof(Schema));
	int i = 0;
	int intSize = sizeof(int);
	BM_PageHandle bufferPageHandle;

	rel->mgmtData = recordMgrData;
	rel->name = name;

	retVal = initBufferPool(&recordMgrData->bufferPool, name, BUFFER_FRAME_SIZE, PAGE_REPLACEMENT_ALGO, NULL);
	if (retVal != RC_OK) {
		printf("BufferPool initialization failed \n");
		return retVal;
	}

	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, META_PAGENO);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	pagePtr = (char*)bufferPageHandle.data;

	recordMgrData->numberOfTuples = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;

	recordMgrData->firstFreePage = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;

	recordMgrData->recordSize = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;

	recordMgrData->numberOfAttr = *((int*)pagePtr);
	schema->numAttr = *((int*)pagePtr);
	pagePtr = pagePtr + intSize;

	// based on attrib num allocate memory for details of attrib
	schema->attrNames = (char**)malloc((sizeof(char*))*(schema->numAttr));
	schema->dataTypes = (DataType*)malloc(sizeof(DataType)*(schema->numAttr));
	schema->typeLength = (int*)malloc(sizeof(int)*schema->numAttr);
	

	//allocate and copy back every attribute's detail
	for (i = 0; i < schema->numAttr; i++) {

		schema->attrNames[i] = (char*)malloc(MAX_ATTRNAME_LEN);

		strncpy( schema->attrNames[i], pagePtr, MAX_ATTRNAME_LEN);
		pagePtr = pagePtr + MAX_ATTRNAME_LEN;

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

	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}
	
	/* enable below function to cross verify data */
	printRecordManager(recordMgrData, schema, "OpenTable");
	return retVal;
}

RC closeTable(RM_TableData *rel)
{
	RC retVal = RC_OK;
	//SM_FileHandle fh;
	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;
	BM_PageHandle bufferPageHandle;

	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, META_PAGENO);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	char* slotPageData = bufferPageHandle.data;

	//First data item in page is numberofTuples, update the field with new data
	*(int*)slotPageData = recordMgrData->numberOfTuples;

	markDirty(&recordMgrData->bufferPool, &bufferPageHandle);

	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	retVal = shutdownBufferPool(&recordMgrData->bufferPool);
	if (retVal != RC_OK) {
		printf("Shutdown buffer pool failed\n");
		return retVal;
	}

	if (rel->mgmtData != NULL) {
		free(rel->mgmtData);
		rel->mgmtData = NULL;
	}
	
	if (rel->schema != NULL) {
		freeSchema(rel->schema);
	}

	return retVal;

}

RC deleteTable(char *name)
{
	RC retVal = RC_OK;
	retVal = destroyPageFile(name);
	if (retVal != RC_OK) {
		printf("destroyPageFile failed");
		return retVal;
	}
	return retVal;
}

int getNumTuples(RM_TableData *rel)
{
	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;
	return recordMgrData->numberOfTuples;
}

int searchForFreeSlot(char* bufferSlotPage, int totalSlotCount, int recordSizeWithDelimiter) {

	int retVal = -1;
	int i = 0;
	for (i = 0; i < totalSlotCount; i++) {
		if (bufferSlotPage[recordSizeWithDelimiter*i] != RECORD_DELIMITER){
			retVal = i;
			return retVal;
		}
	}
	return retVal;
}

// handling records in a table
RC insertRecord(RM_TableData *rel, Record *record) {

	RC retVal = RC_OK;

	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;
	int recordSizeWithDelimiter = recordMgrData->recordSize + 1; // + 1 for extra  delimiter character
	int recordSizeWithoutDelimiter = recordMgrData->recordSize;
	RID *rid = &(record->id);
	char *bufferSlotPage = NULL;
	int totalSlotCount = 0;
	BM_PageHandle bufferPageHandle;

	/**********************Label************************/
	retryWithNextPage:
	// update record id
	rid->page = recordMgrData->firstFreePage;
	rid->slot = -1;

	// pin the free page to write record
	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, rid->page);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	// get the page from buffer slot and insert record
	bufferSlotPage = bufferPageHandle.data;

	totalSlotCount = (int)(PAGE_SIZE / recordSizeWithDelimiter);

	rid->slot = searchForFreeSlot(bufferSlotPage, totalSlotCount, recordSizeWithDelimiter);

	if (rid->slot == -1) {
		//printf("No Free slot in current page retrying with nextPage");
		retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
		if (retVal != RC_OK) {
			printf("Unpin page failed\n");
			return retVal;
		}
		recordMgrData->firstFreePage++;
		goto retryWithNextPage;
	}

	retVal = markDirty(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("markDirty failed\n");
		return retVal;
	}

	// move pointer to slot address
	bufferSlotPage = bufferSlotPage + rid->slot * recordSizeWithDelimiter;

	// add delimiter in beginning
	*bufferSlotPage = RECORD_DELIMITER;
	bufferSlotPage = bufferSlotPage + 1;

	memcpy(bufferSlotPage, record->data, recordSizeWithoutDelimiter);

	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	// copy updated new record id
	record->id = *rid;

	// update totalNoOfTuples
	recordMgrData->numberOfTuples++;
	return retVal;

}

RC deleteRecord(RM_TableData *rel, RID id) {
	RC retVal = RC_OK;
	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;

	int recordSizeWithDelimiter = recordMgrData->recordSize + 1; // + 1 for extra delimiter character
	int recordSizeWithoutDelimiter = recordMgrData->recordSize;
	char *bufferSlotPage = NULL;
	char *tempBufferSlotPage = NULL; // for address movement
	BM_PageHandle bufferPageHandle;
	RID *rid = &id;

	// pin the free page to write record
	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, rid->page);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	//As we are deleting we are updating number of tuples
	recordMgrData->numberOfTuples--;
	bufferSlotPage = bufferPageHandle.data;
	// move to the slot which needs to be deleted
	tempBufferSlotPage = bufferSlotPage + (recordSizeWithDelimiter * rid->slot);
	
	if (TOMBSTONE) {
		// add tombstone to avoid reusing
		*tempBufferSlotPage = RECORD_DELIMITER_TOMB;
	}
	else {
		// we want to reuse deleted record space
		*tempBufferSlotPage = '\0';
	}
	
	retVal = markDirty(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("markDirty failed\n");
		return retVal;
	}

	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	return retVal;
}

RC updateRecord(RM_TableData *rel, Record *record)
{
	RC retVal = RC_OK;
	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;

	int recordSizeWithDelimiter = recordMgrData->recordSize + 1; // + 1 for extra delimiter character
	int recordSizeWithoutDelimiter = recordMgrData->recordSize;
	char *bufferSlotPage = NULL;
	char *tempBufferSlotPage = NULL; // for address movement
	BM_PageHandle bufferPageHandle;
	RID *rid = &(record->id);

	// pin the free page to write record
	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, rid->page);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	bufferSlotPage = bufferPageHandle.data;
	tempBufferSlotPage = bufferSlotPage;

	tempBufferSlotPage = tempBufferSlotPage + recordSizeWithDelimiter * (rid->slot);
	
	// skip delimiter
	tempBufferSlotPage = tempBufferSlotPage + 1;

	memcpy(tempBufferSlotPage, record->data, recordSizeWithoutDelimiter);

	retVal = markDirty(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("markDirty failed\n");
		return retVal;
	}

	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	return retVal;
}

RC getRecord(RM_TableData *rel, RID id, Record *record)
{
	RC retVal = RC_OK;
	RecordMgrData* recordMgrData = (RecordMgrData*)rel->mgmtData;
	RID *rid = &id;
	int recordSizeWithDelimiter = recordMgrData->recordSize + 1; // + 1 for extra delimiter character
	int recordSizeWithoutDelimiter = recordMgrData->recordSize;
	//RID *rid = &(record->id);
	char *bufferSlotPage = NULL;
	char *recordData = NULL;
	BM_PageHandle bufferPageHandle;
	int maxSlots = (int)(PAGE_SIZE / recordSizeWithDelimiter);

	if(rid->slot > maxSlots)
	{
		retVal = RC_RECORD_NOT_FOUND;
		return retVal;
	}

	// pin the page to read record
	retVal = pinPage(&recordMgrData->bufferPool, &bufferPageHandle, rid->page);
	if (retVal != RC_OK) {
		printf("Pin page failed\n");
		return retVal;
	}

	// get the page from buffer slot and insert record
	bufferSlotPage = bufferPageHandle.data;

	// move to the slot in the give page
	bufferSlotPage = bufferSlotPage + (recordSizeWithDelimiter * (rid->slot));

	if (*bufferSlotPage != RECORD_DELIMITER)
	{
		retVal = RC_RECORD_NOT_FOUND;
		unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
		return retVal;
	}
	else {
		// move one character so we wont read delimiter to record data
		bufferSlotPage = bufferSlotPage + 1;
		// updated data
		recordData = record->data;
		memcpy(recordData, bufferSlotPage, recordSizeWithoutDelimiter);
		// updated id
		record->id = id;
	}



	retVal = unpinPage(&recordMgrData->bufferPool, &bufferPageHandle);
	if (retVal != RC_OK) {
		printf("Unpin page failed\n");
		return retVal;
	}

	return retVal;
}

// scans
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	RC retVal = RC_OK;
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)malloc(sizeof(RM_ScaningRecordInfo));
	scan->rel = rel;
	scan->mgmtData= scan_mgmt;
	scan_mgmt->pageCurrentPosition = 1;
	scan_mgmt->slotCurrentPosition = 0;
	scan_mgmt->condition = cond;

	return retVal;
}

RC next(RM_ScanHandle *scan, Record *record)
{
	RC retVal = RC_OK;
	RecordMgrData *recordMgrData = (RecordMgrData *)scan->rel->mgmtData;
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)scan->mgmtData;
	Value *value;
	int recordSizeWithDelimiter = recordMgrData->recordSize + 1; // + 1 for extra  delimiter character
	RC evalRet = RC_OK;
	bool tupleFound = FALSE;

	record->id.page = scan_mgmt->pageCurrentPosition;
	record->id.slot = scan_mgmt->slotCurrentPosition;

	retVal = getRecord(scan->rel, record->id, record); //Get the record information

	while(retVal != RC_RECORD_NOT_FOUND)
	{
		if(scan_mgmt->condition != NULL)
		{
			evalRet = evalExpr(record, scan->rel->schema, scan_mgmt->condition, &value);
			if(evalRet == RC_OK)
			{
				if(value->dt == DT_BOOL && value->v.boolV == TRUE)
				{
					tupleFound =  TRUE;
					break;
				}

				if(value->dt == DT_INT && value->v.intV >= 1)
				{
					tupleFound =  TRUE;
					break;
				}
			}
		}
		else
		{
			tupleFound =  TRUE;
			break;
		}

		if(scan_mgmt->slotCurrentPosition < (int)(PAGE_SIZE / recordSizeWithDelimiter))
		{
			scan_mgmt->slotCurrentPosition++;
		}
		else if (scan_mgmt->pageCurrentPosition <= recordMgrData->firstFreePage)
		{
			scan_mgmt->pageCurrentPosition++;
			scan_mgmt->slotCurrentPosition = 0;
		}
		else
		{
			break;
		}

		record->id.page = scan_mgmt->pageCurrentPosition;
		record->id.slot = scan_mgmt->slotCurrentPosition;
		retVal = getRecord(scan->rel, record->id, record); //Get the record information
	}


	if(tupleFound != TRUE)
	{
		retVal = RC_RM_NO_MORE_TUPLES;
	}
	else
	{
		if(scan_mgmt->slotCurrentPosition < (int)(PAGE_SIZE / recordSizeWithDelimiter))
		{
			scan_mgmt->slotCurrentPosition++;
		}
		else if (scan_mgmt->pageCurrentPosition <= recordMgrData->firstFreePage)
		{
			scan_mgmt->pageCurrentPosition++;
			scan_mgmt->slotCurrentPosition = 0;
		}
	}

	return retVal;
}

RC closeScan(RM_ScanHandle *scan)
{
	RM_ScaningRecordInfo *scan_mgmt = (RM_ScaningRecordInfo *)scan->mgmtData;
	free(scan_mgmt);
	scan->mgmtData = NULL;
	return RC_OK;
}


// dealing with schemas
int getRecordSize(Schema *schema)
{
	int recordSize = 0;
	int i;
	for (i = 0; i < schema->numAttr; i++)
	{
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
			printf("\ngetRecordSize : Shouldn't be here!!\n");
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

	RC retVal = RC_OK;
	int i;

	if(schema != NULL)
	{
		if (schema->dataTypes != NULL) {
			free(schema->dataTypes);
			schema->dataTypes = NULL;
		}

		if (schema->typeLength != NULL) {
			free(schema->typeLength);
			schema->typeLength = NULL;
		}

		if (schema->keyAttrs != NULL) {
			free(schema->keyAttrs);
			schema->keyAttrs = NULL;
		}
	
		if(schema->attrNames != NULL) {
			for(i = 0; i < schema->numAttr; i++)
		    	{
		      		if(schema->attrNames[i] != NULL)
		      			free(schema->attrNames[i]);
		    	}
	    		
	    		free(schema->attrNames);
	    	}

		free(schema);
		schema = NULL;
	}

	return retVal;
}


// dealing with records and attribute values
RC createRecord(Record **record, Schema *schema) {

	RC retVal = RC_OK;
	*record = (Record*)malloc(sizeof(Record));
	(*record)->data = (char*)malloc(getRecordSize(schema));

	(*record)->id.page = -1;
	(*record)->id.slot = -1;

	return retVal;
}

RC freeRecord(Record *record) {

	RC retVal = RC_OK;
	if (record != NULL) {
		if (record->data != NULL) {
			free(record->data);
			record->data = NULL;
		}
		free(record);
	}
	return retVal;

}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {

	RC retVal = RC_OK;
	int offsetInsideRecord = 0;
	char* recordData = record->data;
	int stringLength = 0;
	char* tempBuffer = NULL;
	*value = (Value*)malloc(sizeof(Value));

	findOffsetForAttrNum(attrNum, schema, &offsetInsideRecord);
	// move record pointer to the attrNum position
	recordData = recordData + offsetInsideRecord;

	switch (schema->dataTypes[attrNum])
	{

	case DT_BOOL:
		(*value)->dt = DT_BOOL;
		memcpy(&((*value)->v.boolV), recordData, sizeof(bool));
		break;

	case DT_FLOAT:
		(*value)->dt = DT_FLOAT;
		memcpy(&((*value)->v.floatV), recordData, sizeof(float));
		break;

	case DT_INT:
		(*value)->dt = DT_INT;
		memcpy(&((*value)->v.intV), recordData, sizeof(int));
		break;

	case DT_STRING:
		stringLength = 0;
		stringLength = schema->typeLength[attrNum];
		(*value)->dt = DT_STRING;
		(*value)->v.stringV = (char*)malloc(stringLength + 1);
		strncpy((*value)->v.stringV, recordData, stringLength);
		(*value)->v.stringV[stringLength] = '\0';
		break;

	default:
		printf("setAttr() Shouldn't be here !!!\n");
		break;
	}
	return retVal;

}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {

	RC retVal = RC_OK;
	int offsetInsideRecord = 0;
	char* recordData = record->data;
	int stringLength = 0;
	char* tempBuffer = NULL;

	findOffsetForAttrNum(attrNum, schema, &offsetInsideRecord);

	// move record pointer to the attrNum position
	recordData = recordData + offsetInsideRecord;

	switch (schema->dataTypes[attrNum])
	{

	case DT_BOOL:
		*(bool*)recordData = value->v.boolV;
		break;

	case DT_FLOAT:
		*(float*)recordData = value->v.floatV;
		break;

	case DT_INT:
		*(int*)recordData = value->v.intV;
		break;

	case DT_STRING:
		stringLength = 0;
		stringLength = schema->typeLength[attrNum];
		tempBuffer = (char*)malloc(stringLength + 1);
		strncpy(tempBuffer, value->v.stringV, stringLength);
		// add delimiter at the end
		tempBuffer[stringLength] = '\0';
		strncpy(recordData, tempBuffer, stringLength);
		free(tempBuffer);
		break;

	default:
		printf("setAttr() Shouldn't be here !!!\n");
		break;
	}
	return retVal;

}

// find the offset for the attrNum and give back the offset
RC findOffsetForAttrNum(int attrNum, Schema *schema, int *offset) {

	RC retVal = RC_OK;
	int posOffset = 0;
	int currentPosition = 0;
	DataType dataTypes;

	// while attribute's position is not reached add the typelength /sizeof to current offset
	while (currentPosition < attrNum) {

		// get data type
		dataTypes = schema->dataTypes[currentPosition];
		
		if (dataTypes == DT_STRING) {
			posOffset = posOffset + schema->typeLength[currentPosition];
		}
		else if (dataTypes == DT_FLOAT) {
			posOffset = posOffset + sizeof(float);
		}
		else if (dataTypes == DT_INT) {
			posOffset = posOffset + sizeof(int);
		}
		else if (dataTypes == DT_BOOL) {
			posOffset = posOffset + sizeof(bool);
		}

		currentPosition++;
	}

	*offset = posOffset;
	return retVal;
}
