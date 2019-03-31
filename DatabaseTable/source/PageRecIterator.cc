//
// Created by Daniel on 2/6/2019.
//
#ifndef A2_PAGERECITERATOR_CC
#define A2_PAGERECITERATOR_CC

//#include <DatabaseTable/headers/PageRecIterator.h>

#include "PageRecIterator.h"

// These headers are inluded for testing
#include <cstring>
#include <iostream>
#include <time.h>
#include <unistd.h>
#include <vector>

void MyDB_PageRecIterator::getNext() {
	if(hasNext()){
		void *pos = iterated + myPRW->getData();
		void *nextPos = myRec->fromBinary (pos);
		iterated += ((char *) nextPos) - ((char *) pos);
		// Debug purpose
		//stringstream ss;
		//ss << myRec;
		//cout << ss.str() <<endl;
	}
}

bool MyDB_PageRecIterator::hasNext() {
	if(iterated<myPRW->getStored()){
		return true;
	}else{
		return false;
	}
}

MyDB_PageRecIterator::MyDB_PageRecIterator (MyDB_RecordPtr fromRec, MyDB_PageReaderWriter* fromPRW) {
	myPRW = fromPRW;
	myRec = fromRec;
	iterated = 0;
}


#endif //A2_PAGERECITERATOR_CC

