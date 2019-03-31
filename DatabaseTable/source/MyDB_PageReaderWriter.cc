
#ifndef PAGE_RW_C
#define PAGE_RW_C

//#include <DatabaseTable/headers/MyDB_PageReaderWriter.h>

#include "MyDB_PageReaderWriter.h"

// These headers are inluded for testing
#include <cstring>
#include <iostream>
#include <time.h>
#include <unistd.h>
#include <vector>

MyDB_PageReaderWriter::MyDB_PageReaderWriter(MyDB_PageHandle fromHandle, MyDB_BufferManagerPtr fromBuffer,bool toClear){
	myBuffer = fromBuffer;
	myPage = fromHandle;
	myHeader = (pageInfo*) (myPage->getBytes());
	if(toClear){
		clear();
	}
}

void MyDB_PageReaderWriter :: clear () {
	myHeader = (pageInfo*) (myPage->getBytes());
	myHeader->stored = 0;//(int)(myHeader->data - (char*)myPage->getBytes());
	this->setType(MyDB_PageType::RegularPage);
	myPage->wroteBytes();
}

MyDB_PageType MyDB_PageReaderWriter :: getType () {
	myHeader = (pageInfo*) (myPage->getBytes());
	return myHeader->type;
}

MyDB_RecordIteratorPtr MyDB_PageReaderWriter :: getIterator (MyDB_RecordPtr rec) {
	return make_shared <MyDB_PageRecIterator> (rec, this);
}

void MyDB_PageReaderWriter :: setType (MyDB_PageType newType) {
	myHeader = (pageInfo*) (myPage->getBytes());
	myHeader->type = newType;
	myPage->wroteBytes();
}

bool MyDB_PageReaderWriter :: append (MyDB_RecordPtr newRec) {
	myHeader = (pageInfo*) (myPage->getBytes());
	int recSize = newRec->getBinarySize();
	char* insertLoc = myHeader->data + myHeader->stored;
	int pageSize = myBuffer->getPageSize();
	//cout << (char*)myPage->getBytes()+pageSize-insertLoc-recSize<<endl;
	if(insertLoc+recSize<=(char*)myPage->getBytes()+pageSize){ // if have space, write in
		newRec->toBinary(insertLoc);
		myHeader->stored += recSize;
		//myHeader->stored = (int) (inserted - myHeader->data);
		myPage->wroteBytes();
		return true;
	}else{ // if page don't have enough space
		return false;
	}

}

// Personal Public Helper Function--------------------------------------------------------
int MyDB_PageReaderWriter::getStored() {
	myHeader = (pageInfo*) (myPage->getBytes());
	return myHeader->stored;
}

MyDB_PageHandle MyDB_PageReaderWriter::getPage() {
	myHeader = (pageInfo*) (myPage->getBytes());
	return myPage;
}

char *MyDB_PageReaderWriter::getData() {
	myHeader = (pageInfo*) (myPage->getBytes());
	return myHeader->data;
}


#endif
