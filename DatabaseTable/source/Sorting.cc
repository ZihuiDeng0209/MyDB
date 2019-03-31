
#ifndef SORT_C
#define SORT_C

#include <algorithm>
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

using namespace std;

void mergeIntoFile (MyDB_TableReaderWriter &myTable, vector <MyDB_RecordIteratorAltPtr> &allRuns,
		function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// Initialize
	int size = allRuns.size();
	IteratorComparator myComparator(comparator, lhs, rhs);
	priority_queue<MyDB_RecordIteratorAltPtr,vector<MyDB_RecordIteratorAltPtr>,IteratorComparator> Queue(myComparator);
	//vector <MyDB_RecordIteratorAltPtr> Queue = allRuns;
	for(int i=0;i<size;i++){
		if(allRuns[i]->advance()){
			Queue.push(allRuns[i]);
			//Queue.push_back(allRuns[i]);
		}
	}
	//stable_sort (Queue.begin (), Queue.end (), myComparator);

	MyDB_RecordPtr tempRec = myTable.getEmptyRecord();
	while(!Queue.empty()){
		MyDB_RecordIteratorAltPtr currIter = Queue.top();
		//MyDB_RecordIteratorAltPtr currIter = Queue[0];
		currIter->getCurrent(tempRec);
		myTable.append(tempRec);
		Queue.pop();
		//Queue.erase(Queue.begin());
		if(currIter->advance()){
			Queue.push(currIter);
		}
		//stable_sort (Queue.begin (), Queue.end (), myComparator);
	}
}

vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr thisBuffer, MyDB_RecordIteratorAltPtr iter1,
		MyDB_RecordIteratorAltPtr iter2, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// Initialize
	vector <MyDB_PageReaderWriter> sortedPages;
	//MyDB_PageHandle currPH = thisBuffer->getPinnedPage();
	MyDB_PageReaderWriter currPRW(*thisBuffer);
	bool hasNext1 = iter1->advance();
	bool hasNext2 = iter2->advance();

	// Merge Sort two pages (or page lists)
	while(hasNext1||hasNext2){
		if(!hasNext1){	// if iter1 is done
			iter2->getCurrent(rhs);
			appendNew(thisBuffer,rhs,currPRW,sortedPages);
			hasNext2 = iter2->advance();
		}else if(!hasNext2){	// if iter2 is done
			iter1->getCurrent(lhs);
			appendNew(thisBuffer,lhs,currPRW,sortedPages);
			hasNext1 = iter1->advance();
		}else{
			iter1->getCurrent(lhs);
			iter2->getCurrent(rhs);
			if(comparator()){	// if iter1<iter2
				appendNew(thisBuffer,lhs,currPRW,sortedPages);
				hasNext1 = iter1->advance();
			}else {				// if iter2<iter1
				appendNew(thisBuffer,rhs,currPRW,sortedPages);
				hasNext2 = iter2->advance();
			}
		}
	}
	sortedPages.push_back(currPRW);
	return sortedPages;
}

void appendNew(MyDB_BufferManagerPtr &thisBuffer, MyDB_RecordPtr &rec, MyDB_PageReaderWriter &myPage,
			   vector <MyDB_PageReaderWriter> &sortedPages){
	bool success = myPage.append(rec);
	if(!success){
		// unpin page????
		sortedPages.push_back(myPage);
		MyDB_PageReaderWriter tempPRE(*thisBuffer);
		tempPRE.append(rec);
		myPage = tempPRE;
	}
}

	
void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe,
		   function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// Initialize
	int pageNum = sortMe.getNumPages();
	int count=0;
	vector <MyDB_RecordIteratorAltPtr> allRuns;
	MyDB_BufferManagerPtr thisBuffer = sortMe.getBufferMgr();

	// for each run
	while(count<pageNum){
		// initial run [ [h] [h] [h] ... [h] ]
		vector <vector <MyDB_PageReaderWriter>> currRun;
		for(int i=0;i<runSize;i++){
			if(count<pageNum){
				MyDB_PageReaderWriter currPage = sortMe[count];
				count++;
				MyDB_PageReaderWriterPtr sortedCurr = currPage.sort(comparator,lhs,rhs);
				vector <MyDB_PageReaderWriter> temp;
				temp.push_back(*sortedCurr);
				currRun.push_back(temp);
			}
		}
		// Sort Phase
		int currVecNum = currRun.size();
		vector <vector <MyDB_PageReaderWriter>> toBeMerged = currRun;
		vector <vector <MyDB_PageReaderWriter>> merged;
		// merge until all merged
		while(currVecNum!=1){
			merged.clear();
			int newCount = 0;
			while(newCount<currVecNum){
				// grab first part
				vector <MyDB_PageReaderWriter> toMerge1 = toBeMerged[newCount];
				newCount++;
				// grab second part if possible
				if(newCount<currVecNum){ // has second page
					vector <MyDB_PageReaderWriter> toMerge2 = toBeMerged[newCount];
					newCount++;
					MyDB_RecordIteratorAltPtr iter1 = getIteratorAlt(toMerge1);
					MyDB_RecordIteratorAltPtr iter2 = getIteratorAlt(toMerge2);
					vector <MyDB_PageReaderWriter> tempVec = mergeIntoList(thisBuffer, iter1, iter2, comparator, lhs, rhs);
					merged.push_back(tempVec);
				}else{	// dont has second page
					merged.push_back(toMerge1);
				}
			}
			toBeMerged = merged;
			currVecNum = toBeMerged.size();
		}
		MyDB_RecordIteratorAltPtr currRunIter = getIteratorAlt(toBeMerged[0]);
		allRuns.push_back(currRunIter);
	}

	// Merge Phase
	mergeIntoFile(sortIntoMe,allRuns,comparator,lhs,rhs);
}

#endif
