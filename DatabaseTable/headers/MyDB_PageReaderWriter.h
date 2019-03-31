
#ifndef PAGE_RW_H
#define PAGE_RW_H

#include "MyDB_PageType.h"
#include "MyDB_TableReaderWriter.h"
#include "PageRecIterator.h"

class MyDB_PageReaderWriter {

public:

	// ANY OTHER METHODS YOU WANT HERE
	// Constructor
	MyDB_PageReaderWriter(MyDB_PageHandle fromHandle, MyDB_BufferManagerPtr fromBuffer,bool toClear);
	// get stored data size
	int getStored();
	// get myPage
	MyDB_PageHandle getPage();
	// get data
	char* getData();

	// ------------------------------------------------------------------------------
	// Below are the required methods
	// empties out the contents of this page, so that it has no records in it
	// the type of the page is set to MyDB_PageType :: RegularPage
	void clear ();

	// return an itrator over this page... each time returnVal->next () is
	// called, the resulting record will be placed into the record pointed to
	// by iterateIntoMe
	MyDB_RecordIteratorPtr getIterator (MyDB_RecordPtr iterateIntoMe);

	// appends a record to this page... return false is the append fails because
	// there is not enough space on the page; otherwise, return true
	bool append (MyDB_RecordPtr appendMe);

	// gets the type of this page... this is just a value from an ennumeration
	// that is stored within the page
	MyDB_PageType getType ();

	// sets the type of the page
	void setType (MyDB_PageType toMe);
	
private:

	// ANYTHING ELSE YOU WANT HERE
	// This is the private structure that stores header of the page,
	// a instance of this class will be stored at the beginning of
	// every page
	struct pageInfo {
		int stored;				// size of data already stored
		MyDB_PageType type;		// type of the page
		char data[0];			// Data stored in page
	};

	// Private variables
	MyDB_PageHandle myPage;	// Points to this PRW's page
	MyDB_BufferManagerPtr myBuffer;	// Points to buffer
	struct pageInfo* myHeader;	// Points to the page's storing structure

};

#endif
