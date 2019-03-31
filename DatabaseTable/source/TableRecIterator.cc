//
// Created by Daniel on 2/8/2019.
//
#ifndef A2_TABLERECITERATOR_CC
#define A2_TABLERECITERATOR_CC

//#include <DatabaseTable/headers/TableRecIterator.h>

#include "TableRecIterator.h"

void MyDB_TableRecIterator::getNext() {
	if(hasNext()){
		//cout<<iteratedPageNum<<endl;
		currPage->getNext();
	}
}

bool MyDB_TableRecIterator::hasNext() {
	if(currPage->hasNext()){ // Current page still has records
		return true;
	}else{ // Current page has no record, move to next page who has records
		int scan = iteratedPageNum+1;
		int lastPage = myTRW->getLastNum();
		while(scan<=lastPage){
			MyDB_RecordIteratorPtr tempPage = (*myTRW)[scan].getIterator(myRec);
			if(tempPage->hasNext()){
				currPage = tempPage;
				iteratedPageNum = scan;
				return true;
			}else{
				scan++;
			}
		}
		return false;
	}
}

MyDB_TableRecIterator::MyDB_TableRecIterator (MyDB_RecordPtr fromRec, MyDB_TableReaderWriter* fromTRW) {
	myRec = fromRec;
	myTRW = fromTRW;
	currPage = (*myTRW)[0].getIterator(myRec);
	iteratedPageNum = 0;
}



#endif //A2_TABLERECITERATOR_CC

