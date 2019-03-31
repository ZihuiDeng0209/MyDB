//
// Created by Daniel on 2/6/2019.
//

#ifndef A2_PAGERECITERATOR_H
#define A2_PAGERECITERATOR_H

#include "MyDB_PageReaderWriter.h"
#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"

class MyDB_PageReaderWriter;

class MyDB_PageRecIterator : public MyDB_RecordIterator{

public:
	// put the contents of the next record in the page into the iterator record
	// this should be called BEFORE the iterator record is first examined
	// Override
	virtual void getNext ();

	// return true iff there is another record in the page
	// Override
	virtual bool hasNext ();

	// destructor and constructor
	MyDB_PageRecIterator (MyDB_RecordPtr fromRec, MyDB_PageReaderWriter* fromPRW);
	virtual ~MyDB_PageRecIterator () {};

private:

	int iterated;	// Stores bits that have been iterated thru
	MyDB_PageReaderWriter* myPRW;	// Store this page's PRW
	MyDB_RecordPtr myRec;	// Store reused record class instance


};

#endif //A2_PAGERECITERATOR_H
