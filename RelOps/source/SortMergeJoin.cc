
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"
#include "RecordComparator.h"
#include <queue>

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInputIn, MyDB_TableReaderWriterPtr rightInputIn,
								MyDB_TableReaderWriterPtr outputIn, string finalSelectionPredicateIn,
								vector <string> projectionsIn,
								pair <string, string> equalityCheckIn, string leftSelectionPredicateIn,
								string rightSelectionPredicateIn) {
	leftTable = leftInputIn;
	rightTable = rightInputIn;
	output = outputIn;
	finalSelectionPredicate = finalSelectionPredicateIn;
	projections = projectionsIn;
	equalityCheck = equalityCheckIn;
	leftSelectionPredicate = leftSelectionPredicateIn;
	rightSelectionPredicate = rightSelectionPredicateIn;
}

void SortMergeJoin :: run () {
	// We first do Sort Phase for two tables
	// Create tables to store sorted runs (necessary??????)
	cout << "start SMJ" << endl;
	MyDB_TablePtr tempLeft = make_shared <MyDB_Table> ("tempLeft", "tempLeft.bin", leftTable->getTable()->getSchema());
	MyDB_TablePtr tempRight = make_shared <MyDB_Table> ("tempRight", "tempRight.bin", rightTable->getTable()->getSchema());
	MyDB_TableReaderWriterPtr tempLeftTRW = make_shared <MyDB_TableReaderWriter> (tempLeft, leftTable->getBufferMgr());
	MyDB_TableReaderWriterPtr tempRightTRW = make_shared <MyDB_TableReaderWriter> (tempRight, rightTable->getBufferMgr());
	// -- For leftTable
	cout << "Left table copy" << endl;
	// ---- Create comparator
	MyDB_RecordPtr leftLHS = tempLeftTRW->getEmptyRecord ();
	MyDB_RecordPtr leftRHS = tempLeftTRW->getEmptyRecord ();
	function <bool ()> leftComparator = buildRecordComparator(leftLHS, leftRHS, equalityCheck.first);
	// ---- Copy left table to temp table, meanwhile get rid of recs don't pass leftSelectionPredicate
	MyDB_RecordIteratorAltPtr leftIter = leftTable->getIteratorAlt ();
	MyDB_RecordPtr leftInputRec = tempLeftTRW->getEmptyRecord ();
	func leftPred = leftInputRec->compileComputation (leftSelectionPredicate);
	while(leftIter->advance()) {
		leftIter->getCurrent (leftInputRec);
		if (!leftPred ()->toBool ()) {
			continue;
		}
		tempLeftTRW->append(leftInputRec);
	}
	// ---- Sort Phase of left table
	cout << "Left table sort" << endl;
	MyDB_RecordIteratorAltPtr leftSortedIter = buildItertorOverSortedRuns (int(tempLeftTRW->getBufferMgr()->numPages / 2),
			*tempLeftTRW, leftComparator, leftLHS, leftRHS);

	// -- For rightTable
	cout << "Right table copy" << endl;
	// ---- Create comparator
	MyDB_RecordPtr rightLHS = tempRightTRW->getEmptyRecord ();
	MyDB_RecordPtr rightRHS = tempRightTRW->getEmptyRecord ();
	function <bool ()> rightComparator = buildRecordComparator(rightLHS, rightRHS, equalityCheck.second);
	// ---- Copy left table to temp table, meanwhile get rid of recs don't pass leftSelectionPredicate
	MyDB_RecordIteratorAltPtr rightIter = rightTable->getIteratorAlt ();
	MyDB_RecordPtr rightInputRec = tempRightTRW->getEmptyRecord ();
	func rightPred = rightInputRec->compileComputation (rightSelectionPredicate);
	while(rightIter->advance()) {
		rightIter->getCurrent (rightInputRec);
		if (!rightPred ()->toBool ()) {
			continue;
		}
		tempRightTRW->append(rightInputRec);
	}
	// ---- Sort Phase of left table
	cout << "Right table sort" << endl;
	MyDB_RecordIteratorAltPtr rightSortedIter = buildItertorOverSortedRuns (int(tempRightTRW->getBufferMgr()->numPages / 2),
			*tempRightTRW, rightComparator, rightLHS, rightRHS);

	// Extract all records for easier use
//	cout << "Extract all Records" << endl;
//	queue <MyDB_RecordPtr> leftQueue;
//	queue <MyDB_RecordPtr> rightQueue;
//	while(leftSortedIter->advance()){
//		leftSortedIter->getCurrent(leftInputRec);
//		leftQueue.push(leftInputRec);
//	}
//	cout << "left table recs left: " << leftQueue.size() << endl;
//	while(rightSortedIter->advance()){
//		rightSortedIter->getCurrent(rightInputRec);
//		rightQueue.push(rightInputRec);
//	}
//	cout << "right table recs left: " << rightQueue.size() << endl;

	// Merge two records
	cout << "Prepare to Merge" << endl;
	MyDB_SchemaPtr mySchemaOut = make_shared <MyDB_Schema> ();
	for (auto &p : leftTable->getTable ()->getSchema ()->getAtts ())
		mySchemaOut->appendAtt (p);
	for (auto &p : rightTable->getTable ()->getSchema ()->getAtts ())
		mySchemaOut->appendAtt (p);
	// -- get the combined record
	MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaOut);
	combinedRec->buildFrom (leftInputRec, rightInputRec);
	// -- now, get the final predicate over it
	func finalPredicate = combinedRec->compileComputation (finalSelectionPredicate);
	// -- and get the final set of computatoins that will be used to buld the output record
	vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (combinedRec->compileComputation (s));
	}
	// -- this is the output record
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();

	// Build comparator to check equality between left and right
	string equals = "== (" + equalityCheck.first + " , " + equalityCheck.second + ")";
	string greater = "> (" + equalityCheck.first + " , " + equalityCheck.second + ")";
	string smaller = "< (" + equalityCheck.first + " , " + equalityCheck.second + ")";
	func equalCompare = combinedRec->compileComputation (equals);
	func greaterCompare = combinedRec->compileComputation (greater);
	func lesserCompare = combinedRec->compileComputation (smaller);
	// Build compartor to check self equality
	MyDB_RecordPtr leftInputRecTwo = tempLeftTRW->getEmptyRecord ();
	function <bool ()> leftEquChckOne = buildRecordComparator(leftInputRec, leftInputRecTwo, equalityCheck.first);
	function <bool ()> leftEquChckTwo = buildRecordComparator(leftInputRecTwo, leftInputRec, equalityCheck.first);
	MyDB_RecordPtr rightInputRecTwo = tempRightTRW->getEmptyRecord ();
	function <bool ()> rightEquChckOne = buildRecordComparator(rightInputRec, rightInputRecTwo, equalityCheck.second);
	function <bool ()> rightEquChckTwo = buildRecordComparator(rightInputRecTwo, rightInputRec, equalityCheck.second);
	// Set up storage to store duplicate records
	vector <MyDB_PageReaderWriter> leftDuplicates;
	vector <MyDB_PageReaderWriter> rightDuplicates;
	bool done = false;
	if (!leftSortedIter->advance() || !rightSortedIter->advance()) {
		done = true;
	}

	// Iterate through two tempTables
	cout << "Start Merge" << endl;
	while(!done){
		leftSortedIter->getCurrent(leftInputRec);
		rightSortedIter->getCurrent(rightInputRec);
		if(equalCompare()->toBool()){		// -- If equal
			cout << "Equals!" << endl;
			cout << leftInputRec << " and \n" << rightInputRec << endl;
			leftDuplicates.clear();
			MyDB_PageReaderWriter newLeftPage (true, *(leftTable->getBufferMgr()));
			leftDuplicates.push_back((newLeftPage));
			newLeftPage.append(leftInputRec);
			// Check and store all left duplicate records
			cout << "Check Left Duplicate" << endl;
			bool leftDone = false;
			if (!leftSortedIter->advance()){
				leftDone = true;
			}
			while (!leftDone) {
				cout << "check next";
				leftSortedIter->getCurrent(leftInputRecTwo);
				cout <<" and ";
				if (!leftEquChckOne() && !leftEquChckTwo()) { // If two record equals
					cout << "Next left record is same with last one" << endl;
					if(!newLeftPage.append(leftInputRec)) {
						MyDB_PageReaderWriter nextLeftPage (true, *(leftTable->getBufferMgr()));
						newLeftPage = nextLeftPage;
						leftDuplicates.push_back((newLeftPage));
						newLeftPage.append(leftInputRec);
					}
					if (!leftSortedIter->advance()){
						leftDone = true;
						done = true;
					}
				} else {
					cout << "next left is not the same. Move on..." << endl;
					leftDone = true;
				}
			}
			cout << "Check Right Duplicate" << endl;
			rightDuplicates.clear();
			MyDB_PageReaderWriter newRightPage (true, *(rightTable->getBufferMgr()));
			rightDuplicates.push_back((newRightPage));
			newRightPage.append(rightInputRec);
			bool rightDone = false;
			if (!rightSortedIter->advance()){
				rightDone = true;
			}
			// check and store all right duplicate records
			while (!rightDone) {
				rightSortedIter->getCurrent(rightInputRecTwo);
				if (!rightEquChckOne() && !rightEquChckTwo()) { // If two record equals
					if(!newRightPage.append(rightInputRec)) {
						MyDB_PageReaderWriter nextRightPage (true, *(rightTable->getBufferMgr()));
						newRightPage = nextRightPage;
						rightDuplicates.push_back((newRightPage));
						newRightPage.append(rightInputRec);
					}
					if (!rightSortedIter->advance()){
						rightDone = true;
						done = true;
					}
				} else {
					rightDone = true;
				}
			}
			// Join
			cout << "Join Duplicates" << endl;
			MyDB_RecordIteratorAltPtr newLeftIter = getIteratorAlt(leftDuplicates);
			MyDB_RecordIteratorAltPtr newRightIter;
			while(newLeftIter->advance()) {
				newLeftIter->getCurrent(leftInputRec);
				newRightIter = getIteratorAlt(rightDuplicates);
				while(newRightIter->advance()){
					newRightIter->getCurrent(rightInputRec);
					if(equalCompare()->toBool()) {
						if (finalPredicate ()->toBool ()) {
							int i = 0;
							for (auto &f : finalComputations) {
								outputRec->getAtt (i++)->set (f());
							}
							outputRec->recordContentHasChanged ();
							output->append (outputRec);
						}
					} else {
						break;
					}
				}
			}
		} else {	// -- If not equal, advance either side
			if (greaterCompare()->toBool()){
				cout << "right smaller" << endl;
				if (!rightSortedIter->advance()) {
					done = true;
				}
			} else if (lesserCompare()->toBool()) {
				cout << "left smaller" << endl;
				if (!leftSortedIter->advance()) {
					done = true;
				}
			}
		}
	}
	// Clean tempTables


}


#endif
