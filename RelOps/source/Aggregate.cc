
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

using namespace std;

Aggregate :: Aggregate (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
						vector <pair <MyDB_AggType, string>> aggsToComputeIn,
						vector <string> groupingsIn, string selectionPredicateIn) {
	input = inputIn;
	output = outputIn;
	aggsToCompute = aggsToComputeIn;
	groupings = groupingsIn;
	selectionPredicate = selectionPredicateIn;
}

void Aggregate :: run () {
	// HashTable to store all groupings
	unordered_map <size_t, vector <void *>> myHash;

	// Set up grouping calculations
	// This record is used to iterate input table
	MyDB_RecordPtr inputRec = input->getEmptyRecord();
	vector <func> groupingCalc;
	for (auto &p : groupings) {
		groupingCalc.push_back (inputRec->compileComputation (p));
	}

	// Get Schema to store calculating records in Hash table
	MyDB_SchemaPtr mySchemaAgg = make_shared <MyDB_Schema> ();
	int i = 0;
	int j = 0;
	for (auto &p : output->getTable ()->getSchema ()->getAtts ()) {
		if(i < groupings.size()) {
			mySchemaAgg->appendAtt (make_pair("GroupBy" + to_string(i), p.second));
			i++;
		} else {
			mySchemaAgg->appendAtt (make_pair("Aggregation" + to_string(j), p.second));
			j++;
		}
	}
	mySchemaAgg->appendAtt (make_pair("Count", make_shared <MyDB_IntAttType> ()));
	// -- This rec is what stored in hash table
	MyDB_RecordPtr aggRec = make_shared <MyDB_Record> (mySchemaAgg);

	// Get Schema that combines input and aggSchema so that we can compare groupings between input and hashed recs
	MyDB_SchemaPtr mySchemaCombined = make_shared <MyDB_Schema> ();
	for (auto &p : input->getTable ()->getSchema ()->getAtts ())
		mySchemaCombined->appendAtt (p);
	for (auto &p : mySchemaAgg->getAtts ())
		mySchemaCombined->appendAtt (p);
	MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (mySchemaCombined);
	// -- This rec is used to compare groupings
	combinedRec->buildFrom (inputRec,aggRec);

	// Set up grouping compare calculation
	string groupComparePred = "bool[true]";
	int k = 0;
	for (auto &p : groupings) {
		string addOn = "== (" + p + ", [GroupBy" + to_string(k) + "])";
		if(groupComparePred == "bool[true]") {
			groupComparePred = addOn;
		} else {
			groupComparePred = "&& (" + groupComparePred + ", " + addOn + ")";
		}
		k++;
	}
	// -- This function is used to compare groupings
	func groupCompare = combinedRec->compileComputation(groupComparePred);

	// WHERE clause
	func selectPred = inputRec->compileComputation (selectionPredicate);

	// Set up aggregation calculations
	int x = 0;
	vector <func> aggCalc;
	for (auto &p : aggsToCompute) {
		if (p.first == MyDB_AggType::sum || p.first == MyDB_AggType::avg) {
			string sumCalcPred = "+([Aggregation" + to_string(x) + "], " + p.second + ")";
			aggCalc.push_back(combinedRec->compileComputation(sumCalcPred));
		}
		x++;
	}
	string countCalcPred = "+([Count], int[1])";
	aggCalc.push_back(combinedRec->compileComputation(countCalcPred));

	// -------------------------------------------------------------------------------------
	// Prepare to iterate
	vector <MyDB_PageReaderWriter> allOutputAggRecs;
	MyDB_PageReaderWriter newPage (true, *(input->getBufferMgr()));
	allOutputAggRecs.push_back(newPage);
	MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt();

	// Iterate
	while(myIter->advance()) {
		// get next rec
		myIter->getCurrent(inputRec);
		// Check WHERE Clause
		if(!selectPred()->toBool()) {
			continue;
		}
		// Hash current rec's grouping
		size_t hashVal = 0;
		for (auto &f : groupingCalc) {
			hashVal ^= f ()->hash ();
		}

		// If there is no match in hash table,
		if (myHash.count (hashVal) == 0) {
			// Set up record value
			int y = 0;
			for (auto &f : groupingCalc) {
				aggRec->getAtt (y++)->set (f());
			}
			for (auto &f : aggCalc) {
				aggRec->getAtt (y++)->set (f());
			}
			// Set up storage in page
			
		}

		// if there is a match, then get the list of matches
		vector <void *> &potentialMatches = myHash [hashVal];

	}





}

#endif

