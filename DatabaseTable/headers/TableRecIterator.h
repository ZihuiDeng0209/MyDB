//
// Created by Daniel on 2/8/2019.
//

#ifndef A2_TABLERECITERATOR_H
#define A2_TABLERECITERATOR_H

#include "MyDB_TableReaderWriter.h"
#include "MyDB_RecordIterator.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"

class MyDB_TableReaderWriter;

class MyDB_TableRecIterator : public MyDB_RecordIterator{

public:
	// put the contents of the next record in the table into the iterator record
	// this should be called BEFORE the iterator record is first examined
	// Override
	virtual void getNext ();

	// return true iff there is another record in the table
	// Override
	virtual bool hasNext ();

	// destructor and constructor
	MyDB_TableRecIterator (MyDB_RecordPtr fromRec, MyDB_TableReaderWriter* fromTRW);
	virtual ~MyDB_TableRecIterator () {};

private:

	MyDB_RecordPtr myRec;	// Store reused record class instance
	MyDB_TableReaderWriter* myTRW;	// Store this table's TRW
	MyDB_RecordIteratorPtr currPage;	// Store PRW of current iterating page
	int iteratedPageNum;	// num of page already iterated

};

#endif //A2_TABLERECITERATOR_H
