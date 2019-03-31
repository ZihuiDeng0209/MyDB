// version 0116.1

#ifndef BUFFER_MGR_H
#define BUFFER_MGR_H

#include "MyDB_PageHandle.h"
#include "MyDB_Table.h"
#include <unordered_map>

using namespace std;

class Page;
class Node;
class MyDB_PageHandleBase;
typedef shared_ptr <MyDB_PageHandleBase> MyDB_PageHandle;

class MyDB_BufferManager {

	class lruList;

public:

	// THESE METHODS MUST APPEAR AND THE PROTOTYPES CANNOT CHANGE!

	// gets the i^th page in the table whichTable... note that if the page
	// is currently being used (that is, the page is current buffered) a handle 
	// to that already-buffered page should be returned
	MyDB_PageHandle getPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page that will no longer exist (1) after the buffer manager
	// has been destroyed, or (2) there are no more references to it anywhere in the
	// program.  Typically such a temporary page will be used as buffer memory.
	// since it is just a temp page, it is not associated with any particular 
	// table
	MyDB_PageHandle getPage ();

	// gets the i^th page in the table whichTable... the only difference 
	// between this method and getPage (whicTable, i) is that the page will be 
	// pinned in RAM; it cannot be written out to the file
	MyDB_PageHandle getPinnedPage (MyDB_TablePtr whichTable, long i);

	// gets a temporary page, like getPage (), except that this one is pinned
	MyDB_PageHandle getPinnedPage ();

	// un-pins the specified page
	void unpin (MyDB_PageHandle unpinMe);

	// creates an LRU buffer manager... params are as follows:
	// 1) the size of each page is pageSize 
	// 2) the number of pages managed by the buffer manager is numPages;
	// 3) temporary pages are written to the file tempFile
	MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile);
	
	// when the buffer manager is destroyed, all of the dirty pages need to be
	// written back to disk, any necessary data needs to be written to the catalog,
	// and any temporary files need to be deleted
	~MyDB_BufferManager ();

	// FEEL FREE TO ADD ADDITIONAL PUBLIC METHODS
	// ---------------------------------------------------------------------------
	// Access LRU structure in buffer manager
	MyDB_BufferManager :: lruList* getLRU();
	// get the size of each page
	size_t getPageSize();
	// find next empty slot in buffer, return -1 if none
	int findEmptySlot();
	// Set slot to full
	void changeEmptySlot(int i, bool empty);
	// get buffer pool loc
	char* getBuffer();
	// get Hashtable
	unordered_map<string,unordered_map<long,Page*>*>* getMap();
	// Evict a page from buffer, return the slot that is evict
	int evict();
	// End of public helper function -----------------------------------------------

	//Test purpose
	int numPin; // number of pages pinned in the buffer
	int numPgs;	// number of pages in the buffer

private:

	// YOUR STUFF HERE
	// buffer pool variables
	size_t pageSize; // size of page
	size_t numPages; // number of page
	string tempFile; // temp file name?location
	char* bufferPool; //buffer pool
	MyDB_BufferManager :: lruList* LRU;
	bool* emptySlots;
	//unordered_map<long,Page*>* pageMap; //mapping page number to page (buffered)
	unordered_map<string,unordered_map<long,Page*>*>* fileToPageMap; // mapping table to pageMap
	//unordered_map<long,Page*>* allPageMap; //mapping all existing page number to page (in buffer or not)
	unordered_map<string,unordered_map<long,Page*>*>* fileToAllPageMap; // mapping table to allPageMap
	unordered_map<string,int>* fileMap; //mapping table to file
	int tempPageStoreCount;

	// helper function
	// find a page in buffer, return return null if not in
	Page* findInBuffer(string tableNm, long i);
	// End of private helper function---------------------------------------------

	// this is the double linked list to store LRU, note that head and tail are
	// sentinel nodes. At first, list only contains head and tail, each time a
	// page is added into the buffer, add a node, and vice versa. LRU end is the head
	// i.e. remove from head
	class lruList {
	public:
		Node* head;
		Node* tail;
		int pageCount;
		// constructor
		lruList();
		// add a page into the buffer
		Node* addPage(Page* page);
		// when delete a page from buffer
		void removePage(Node* remove);
		// move page to MRU (tail)
		void MRUPage(Node *move);
		// print LRU
		void print();
		// destructor
		~lruList();
	};
	// End of LRU classes---------------------------------------------------------



};

// This is a private class Page to store all page information and pointers
// to allocate page in table and buffer. Note that page bytes are not stored
// in this class, but in buffer itself.
class Page {
public:
	int pageLoc;	// location of page in file
	int file; // file indicator
	string tableNm;
	char* toBuffer;	// points to position in buffer
	long pgNum;		// the number of page in WhichTable that this Page stores
	int bufferNm;	// num of buffer that the page is stored in
	Node* toLRU;	// points to LRU list in buffer Manager
	bool pinned;
	bool dirty;
	bool tempPage;
	int handleNum;	// number of handles points to Page
	MyDB_BufferManager* myBufferMng;	// Points to buffer manager itself
	
	// page constructor, takes in page location in table and whether page is pinned
	Page(long i, int fileloc, string tableNm, int locOfPage, bool pin=false);
	// default constructor (temp page initializer)
	Page(int fileloc);
	// add handle number
	void addHandleNum();
	// substract handle number, if handle number reach zero, do proper thing
	void delHandleNum();
	// mark page as dirty
	void dirtyPage();
	// pin page, return true if pinned, false if it is already pinned
	bool pinPage();
	// unpin page, return true if unpinned, false if it is already unpinned
	bool unpinPage();
	// Set Buffer Manager
	void setManager(MyDB_BufferManager* myBuffer);
	// direct to buffer, copy table data to buffer
	void setBuffer(char* locInBuffer);
	// direct to buffer
	void unsetBuffer();
	// check whether the page is buffered
	bool buffered();
	// destructor
	~Page();
};
// End of class Page------------------------------------------------------

class Node{
public:
	Node* prev;
	Node* next;
	Page* page;
	bool sentinel;
};

#endif


