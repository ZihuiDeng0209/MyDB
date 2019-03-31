
#ifndef TABLE_RW_C
#define TABLE_RW_C

#include <fstream>
#include <string>
//#include <DatabaseTable/headers/MyDB_TableReaderWriter.h>

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include <string>
#include <sstream>
#include <cstring>
#include <iostream>
#include <time.h>
#include <unistd.h>
#include <vector>

using namespace std;

MyDB_TableReaderWriter :: MyDB_TableReaderWriter (MyDB_TablePtr toTable, MyDB_BufferManagerPtr toBuffer){
	myBuffer = toBuffer;
	myTable = toTable;
	int lastPage = myTable->lastPage();
	if(lastPage!=-1){	// Table had been modified, need to build pageReaderWriter for all modified pages
		for (int i=0;i<=lastPage;i++){
			MyDB_PageHandle tempPage = myBuffer->getPage(myTable,i);
			pages.push_back(MyDB_PageReaderWriter(tempPage,myBuffer,false));
		}
	}
}

MyDB_PageReaderWriter &MyDB_TableReaderWriter :: operator [] (size_t pageNum) {
	// if out of index (after last page), create those pages
	if(pageNum > myTable->lastPage()){
		for(int i=myTable->lastPage()+1;i<=pageNum;i++){
			MyDB_PageHandle tempPage = myBuffer->getPage(myTable,i);
			myTable->setLastPage((size_t)i);
			pages.push_back(MyDB_PageReaderWriter(tempPage,myBuffer,true));
		}
	}
	return pages[pageNum];
}

MyDB_RecordPtr MyDB_TableReaderWriter :: getEmptyRecord () {
	return make_shared <MyDB_Record> (myTable->getSchema());
}

MyDB_PageReaderWriter &MyDB_TableReaderWriter :: last () {
	if(myTable->lastPage()==-1){	// if no page yet, create new and return
		MyDB_PageHandle tempPage = myBuffer->getPage(myTable,0);
		pages.push_back(MyDB_PageReaderWriter(tempPage,myBuffer,true));
		myTable->setLastPage((size_t)0);
	}
	return pages[myTable->lastPage()];
}


void MyDB_TableReaderWriter :: append (MyDB_RecordPtr rec) {
	bool success = last().append(rec);
	if(!success){
		MyDB_PageHandle tempPage = myBuffer->getPage(myTable,myTable->lastPage()+1);
		MyDB_PageReaderWriter newPRW(tempPage,myBuffer,true);
		newPRW.append(rec);
		pages.push_back(newPRW);
		myTable->setLastPage((size_t)myTable->lastPage()+1);
	}

}

void MyDB_TableReaderWriter :: loadFromTextFile (string fromMe) {
	// reset everything
	pages.clear();
	myTable->setLastPage((size_t)-1);
	MyDB_PageReaderWriter newPage = last();
	MyDB_RecordPtr newRec = getEmptyRecord();

	// Reference: http://www.cplusplus.com/doc/tutorial/files/
	string line;
	ifstream importFile (fromMe);
	if (importFile.is_open()) {
		while (getline(importFile, line)) {
			//cout<<line<<endl;
			newRec->fromString(line);
			append(newRec);
		}
		importFile.close();
	}else {
		cout << "Could not read in from file "+fromMe << endl;
	}

}

MyDB_RecordIteratorPtr MyDB_TableReaderWriter :: getIterator (MyDB_RecordPtr rec) {
	return make_shared <MyDB_TableRecIterator> (rec,this);
}

void MyDB_TableReaderWriter :: writeIntoTextFile (string toMe) {
	MyDB_RecordPtr temp = getEmptyRecord();

	// Reference: http://www.cplusplus.com/doc/tutorial/files/
	ofstream exportFile (toMe);
	if (exportFile.is_open()){
		MyDB_RecordIteratorPtr outTemp = getIterator(temp);
		while(outTemp->hasNext()){
			outTemp->getNext();
			stringstream ss;
			ss << temp;
			//cout << ss.str() <<endl;
			exportFile << ss.str() << endl;
		}
		exportFile.close();
	}else {
		cout << "Could not write to file "+toMe << endl;
	}
}

// Personal Public Helper Function--------------------------------------------------------
int MyDB_TableReaderWriter::getLastNum() {
	return myTable->lastPage();
}

#endif

