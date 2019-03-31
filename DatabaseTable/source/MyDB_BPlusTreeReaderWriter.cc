
#ifndef BPLUS_C
#define BPLUS_C

#include <MyDB_BPlusTreeReaderWriter.h>

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

// ---------------------------PUBLIC METHODS---------------------------------------------------------------------------
MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();

	// Clear all things
	this->getTable()->setLastPage(0);
	rootLocation = 0;
	this->getTable()->setRootLocation(rootLocation);
	// initialize root
	MyDB_PageReaderWriter rootPage = (*this)[0];
	rootPage.clear();
	rootPage.setType(DirectoryPage);
	// initialize child
	MyDB_PageReaderWriter childPage = (*this)[1];
	childPage.clear();
	childPage.setType(RegularPage);
	MyDB_INRecordPtr infRec = this->getINRecord();
	infRec->setPtr(1);
	rootPage.append(infRec);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return this->getIteratorHelper(low, high, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {
	return this->getIteratorHelper(low, high, false);
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe) {
	// cout << "get into append"<<endl;
	// Append the record
	MyDB_RecordPtr splitted = this->append(rootLocation, appendMe);
	// If root needs a split
	if (splitted != nullptr) {
		int originalRoot = this->getTable()->getRootLocation();
		// Set up new root
		rootLocation = this->getTable()->lastPage() + 1;
		this->getTable()->setLastPage(rootLocation);
		this->getTable()->setRootLocation(rootLocation);
		MyDB_PageReaderWriter rootPage = (*this)[rootLocation];
		rootPage.clear();
		rootPage.setType(DirectoryPage);
		// Insert all rec to new root
		MyDB_INRecordPtr oldRoot = this->getINRecord();
		oldRoot->setPtr(originalRoot);
		rootPage.append(splitted);
		rootPage.append(oldRoot);
		// Sort the new root 
		MyDB_RecordPtr lhs = this->getEmptyRecord();
		MyDB_RecordPtr rhs = this->getEmptyRecord();
		function <bool ()> comparator = this->buildComparator(lhs, rhs);
		rootPage.sortInPlace(comparator, lhs, rhs);
	}
	// printTree();
}

void MyDB_BPlusTreeReaderWriter :: printTree () {
	printTreeHelper(rootLocation,0);
	cout << "---------END---------" << endl;
}

// ---------------------------PRIVATE METHODS---------------------------------------------------------------------------
bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list,
												  MyDB_AttValPtr low, MyDB_AttValPtr high) {
	// Get starting page
	MyDB_PageReaderWriter currPage = (*this)[whichPage];
	// cout << low->toInt() << endl;
	// cout << high->toInt() << endl;
	if (currPage.getType() == DirectoryPage) {	// if this is an internal page
		MyDB_RecordIteratorAltPtr iter = currPage.getIteratorAlt();
		// Set up comparators
		MyDB_INRecordPtr highRec = this->getINRecord();
		highRec->setKey(high);
		MyDB_INRecordPtr lowRec = this->getINRecord();
		lowRec->setKey(low);
		// cout << "high bound: " << (MyDB_RecordPtr) highRec << endl;
		// cout << "low bound: " << (MyDB_RecordPtr) lowRec << endl;
		MyDB_INRecordPtr currRec = this->getINRecord();
		function <bool ()> greaterThanLow = buildComparator(lowRec, currRec);
		function <bool ()> lesserThanLow = buildComparator(currRec, lowRec);
		function <bool ()> lesserThanHigh = buildComparator(currRec, highRec);
		function <bool ()> greaterThanHigh = buildComparator(highRec, currRec);
		bool advanceOneMore = true;
		// iterate though all INRec of this page
		while(iter->advance()){
			iter->getCurrent(currRec);
			// cout << "current record " << (MyDB_RecordPtr) currRec << endl;
			if((greaterThanLow() && lesserThanHigh()) ||
				(!greaterThanLow() && !lesserThanLow()) ||
				(!greaterThanHigh() && !lesserThanHigh())) {					 // if within given range (including boundaries)
				int childPage = currRec->getPtr();
				discoverPages(childPage, list, low, high);
			} else if(greaterThanLow() && !lesserThanHigh() && advanceOneMore) { // need to reach out of range for one record
				int childPage = currRec->getPtr();
				discoverPages(childPage, list, low, high);
				advanceOneMore = false;
			}
		}
		return false;
	} else {									// if this is a leaf page
		list.push_back(currPage);
		return true;
	}
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getIteratorHelper (MyDB_AttValPtr low, MyDB_AttValPtr high, bool sort) {
	// Prepare for calling iterator
	vector <MyDB_PageReaderWriter> list;
	this->discoverPages(rootLocation, list, low, high);
	if(list.empty()) {
		cout << "No record in given range! Please re-check your range!" << endl;
		exit (1);
	}
	// cout << list.size() << endl;
	MyDB_RecordPtr lhs = this->getEmptyRecord();
	MyDB_RecordPtr rhs = this->getEmptyRecord();
	function <bool ()> comparator = this->buildComparator(lhs, rhs);
	MyDB_RecordPtr myRec = this->getEmptyRecord();
	MyDB_INRecordPtr lowRec = this->getINRecord();
	lowRec->setKey(low);
	function <bool ()> lowComparator = this->buildComparator(myRec, lowRec);
	MyDB_INRecordPtr highRec = this->getINRecord();
	highRec->setKey(high);
	function <bool ()> highComparator = this->buildComparator(highRec, myRec);

	// Build iterator
	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(list, lhs, rhs, comparator, myRec,
															lowComparator, highComparator, sort);
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
	// string ll = splitMe.getType()==RegularPage?"Leaf Page":"Internal Page";
	// cout<<"Now Split a "<< ll <<endl;
	// Generate New Page to Store Half of the Data
	int newPageNum = this->getNumPages() + 1;
	MyDB_PageReaderWriter newPage = (*this)[newPageNum];
	newPage.clear();
	newPage.setType(splitMe.getType());
	MyDB_INRecordPtr splittingNode = getINRecord();
	// Split
	if(newPage.getType() == RegularPage) {	// If at Leaf
		// Sort the Page
		MyDB_RecordPtr lhs = this->getEmptyRecord();
		MyDB_RecordPtr rhs = this->getEmptyRecord();
		function <bool ()> comparator = this->buildComparator(lhs, rhs);
		splitMe.sortInPlace(comparator, lhs, rhs);
		// Extract Records from splitMe
		bool added = false; // New Record Added to RecList or Not
		vector <MyDB_RecordPtr> allRecs; // Stores all Records
		MyDB_RecordIteratorAltPtr iter = splitMe.getIteratorAlt();
		while(iter->advance()) {
			MyDB_RecordPtr currRec = this->getEmptyRecord();
			iter->getCurrent(currRec);
			function <bool ()> addNewComparator = this->buildComparator(andMe, currRec);
			if(addNewComparator() && !added){
				allRecs.push_back(andMe);
				added = true;
			}
			allRecs.push_back(currRec);
		}
		// If New Rec is Largest
		if(!added) {
			allRecs.push_back(andMe);
			added = true;
		}
		int numOfRecs = allRecs.size();
		int halfOfRecs = numOfRecs / 2;
		// Clear Old SplitMe
		splitMe.clear();
		splitMe.setType(RegularPage);
		// Insert all Records into Two Pages
		for (int i = 0; i < numOfRecs; i++){
			if(i < halfOfRecs) {
				newPage.append(allRecs[i]);
			} else if (i == halfOfRecs) {
				// This one is splitting node
				splittingNode->setKey(getKey(allRecs[i]));
				splittingNode->setPtr(newPageNum);
				splitMe.append(allRecs[i]);
			} else {
				splitMe.append(allRecs[i]);
			}
		}
		return splittingNode;
	} else {								// If at Internal Page
		// Same as Leaf Nodes, except we append SplittingNode in newPage
		// Sort the Page
		MyDB_RecordPtr lhs = this->getINRecord();
		MyDB_RecordPtr rhs = this->getINRecord();
		function <bool ()> comparator = this->buildComparator(lhs, rhs);
		splitMe.sortInPlace(comparator, lhs, rhs);
		// Extract Records from splitMe
		bool added = false; // New Record Added to RecList or Not
		vector <MyDB_RecordPtr> allRecs; // Stores all Records
		MyDB_RecordIteratorAltPtr iter = splitMe.getIteratorAlt();
		while(iter->advance()) {
			MyDB_RecordPtr currRec = this->getINRecord();
			iter->getCurrent(currRec);
			function <bool ()> addNewComparator = this->buildComparator(andMe, currRec);
			if(addNewComparator() && !added){
				allRecs.push_back(andMe);
				added = true;
			}
			allRecs.push_back(currRec);
		}
		// If New Rec is Largest
		if(!added) {
			allRecs.push_back(andMe);
			added = true;
		}
		int numOfRecs = allRecs.size();
		int halfOfRecs = numOfRecs / 2;
		// Clear Old SplitMe
		splitMe.clear();
		splitMe.setType(DirectoryPage);
		// Insert all Records into Two Pages
		for (int i = 0; i < numOfRecs; i++){
			if(i < halfOfRecs) {
				newPage.append(allRecs[i]);
			} else if (i == halfOfRecs) {
				// This one is splitting node
				splittingNode->setKey(getKey(allRecs[i]));
				splittingNode->setPtr(newPageNum);
				newPage.append(allRecs[i]);
			} else {
				splitMe.append(allRecs[i]);
			}
		}
	}
	return splittingNode;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe) {
	// cout << "get into append helper appending to "<<whichPage<<endl;
	// Initialize current page and comparator 
	MyDB_PageReaderWriter currPage = (*this)[whichPage];
	MyDB_INRecordPtr toCompare = this->getINRecord();
	function <bool ()> comparator = this->buildComparator(toCompare, appendMe);
    // cout<<"- Start to append"<<endl;
	if (currPage.getType() == DirectoryPage) {	// If internal node
		// cout << "-- page is directory page"<<endl;
		MyDB_RecordIteratorAltPtr iter = currPage.getIteratorAlt();
		while (iter->advance()){
			iter->getCurrent(toCompare);
			// If get to correct node (toCompare >= appendMe)
			if(!comparator()) {
				// try to append (recursively)
				MyDB_RecordPtr toAppend = this->append(toCompare->getPtr(), appendMe);
				if(toAppend == nullptr) {				// If appended in successfully
					return nullptr;
				} else if(currPage.append(toAppend)) {	// If child node splits, but we have space to store that record
					MyDB_RecordPtr lhs = this->getINRecord();
					MyDB_RecordPtr rhs = this->getINRecord();
					function <bool ()> sortComparator = this->buildComparator(lhs, rhs);
					currPage.sortInPlace(sortComparator, lhs, rhs);
					return nullptr;
				} else {								//  If child node splits, and we need a split as well
					return this->split(currPage, toAppend);
				}
			}
		}
	} else {									// If leaf node
		// cout << "-- page is leaf page"<<endl;
		// cout<<appendMe<<endl;
		if(currPage.append(appendMe)) {	// If we can handle this
			return nullptr;
		} else {						// If we need a split
			return this->split(currPage, appendMe);
		}
	}
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

void MyDB_BPlusTreeReaderWriter::printTreeHelper(int pageNum, int level) {
	// Print a laid down tree
	MyDB_PageReaderWriter currPage = (*this)[pageNum];
	if (currPage.getType() == DirectoryPage) {    // If internal node
		MyDB_RecordIteratorAltPtr iter = currPage.getIteratorAlt();
		MyDB_INRecordPtr myRec = getINRecord();
		// for each record, print out itself, then recursively print out its children
		while (iter->advance()){
			iter->getCurrent(myRec);
			for(int i = 0; i < level; i++){
				cout << "||" << "\t";
			}
			cout << "***||level " << level << ": " << (MyDB_RecordPtr) myRec << "---------" << endl;
			this->printTreeHelper(myRec->getPtr(), level + 1);
		}
	} else {									// If leaf node
		MyDB_RecordIteratorAltPtr iter = currPage.getIteratorAlt();
		MyDB_RecordPtr myRec = getEmptyRecord();
		// for each record, print out itself
		while (iter->advance()){
			iter->getCurrent(myRec);
			for(int i = 0; i < level; i++){
				cout << "||" << "\t";
			}
			cout << "||Leaf: " << myRec << endl;
		}
	}
}


#endif
