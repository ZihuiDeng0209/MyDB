// version 0124.1

#ifndef BUFFER_MGR_C
#define BUFFER_MGR_C

#include "MyDB_BufferManager.h"
#include <string>
#include <sys/stat.h>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <utility>
#include <stdio.h>


using namespace std;

// PUBLIC FUNCTION for buffer manager------------------------------------------------
MyDB_PageHandle MyDB_BufferManager :: getPage (MyDB_TablePtr whichTable, long i) {
	string tableNm = whichTable->getName();
	string fileLoc = whichTable->getStorageLoc();
	// create/open file based on table
	int file = 0;
	if (fileMap->find(tableNm) == fileMap->end()){ // file haven't created
		file = open (fileLoc.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);// O_FSYNC
		fileMap->insert({tableNm,file});
		fileToPageMap->insert({tableNm,new unordered_map<long,Page*>});
		fileToAllPageMap->insert({tableNm,new unordered_map<long,Page*>});
	}else{ // file created
		file =  fileMap->find(tableNm)->second;
	}
	// allocate page
	Page* curr = findInBuffer(tableNm,i);
	if(curr!= nullptr){ // page in buffer
		// curr->myBufferMng->getLRU()->MRUPage(curr->toLRU);
		return make_shared <MyDB_PageHandleBase> (curr);
	}
	else{ // page not in buffer
		// find if page already exist out of the buffer
		unordered_map<long,Page*>* allPageMap = fileToAllPageMap->find(tableNm)->second;
		if(allPageMap->find(i) == allPageMap->end()){ // don't exist out of buffer
			Page* newPage = new Page(i,file,tableNm,i*pageSize,false);
			newPage->setManager(this);
			allPageMap->insert({i,newPage});
			return make_shared <MyDB_PageHandleBase> (newPage);
		}else{
			curr = allPageMap->find(i)->second;
			return make_shared <MyDB_PageHandleBase> (curr);
		}
	}
}

MyDB_PageHandle MyDB_BufferManager :: getPage () {
	int file = 0;
	if (fileMap->find("tempPageFile") == fileMap->end()){
		file = open (tempFile.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		fileMap->insert({"tempPageFile",file});
	}else{
		file =  fileMap->find("tempPageFile")->second;
	}
	Page* newPage = new Page(file);
	newPage->setManager(this);
	return make_shared <MyDB_PageHandleBase> (newPage);
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage (MyDB_TablePtr whichTable, long i) {
	string tableNm = whichTable->getName();
	string fileLoc = whichTable->getStorageLoc();
	// create/open file based on table
	int file = 0;
	if (fileMap->find(tableNm) == fileMap->end()){ // file haven't created
		file = open (fileLoc.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		fileMap->insert({tableNm,file});
		fileToPageMap->insert({tableNm,new unordered_map<long,Page*>});
		fileToAllPageMap->insert({tableNm,new unordered_map<long,Page*>});
	}else{ // file created
		file =  fileMap->find(tableNm)->second;
	}
	// allocate page
	Page* curr = findInBuffer(tableNm,i);
	if(curr!= nullptr){ // page in buffer
		curr->pinPage();
		// curr->myBufferMng->getLRU()->MRUPage(curr->toLRU);
		return make_shared <MyDB_PageHandleBase> (curr);
	}
	else{ // page not in buffer
		// find if page already exist out of the buffer
		unordered_map<long,Page*>* allPageMap = fileToAllPageMap->find(tableNm)->second;
		if(allPageMap->find(i) == allPageMap->end()){ // not exist anywhere
			curr = new Page(i,file,tableNm,i*pageSize,false);
			curr->setManager(this);
			allPageMap->insert({i,curr});
		}else{ //exist out of buffer
			curr = allPageMap->find(i)->second;
		}
    	// insert to buffer
		int pos = findEmptySlot();
		if(pos==-1){ // Buffer is full, evict one
			pos = evict();
		}
		// insert in
		changeEmptySlot(pos,false);
		unordered_map<long,Page*>* pageMap = fileToPageMap->find(curr->tableNm)->second;
		pageMap->insert({curr->pgNum,curr});
		curr->bufferNm = pos;
		curr->toBuffer = bufferPool+pos*pageSize;
		curr->setBuffer(curr->toBuffer);
		return make_shared <MyDB_PageHandleBase> (curr);
	}
}

MyDB_PageHandle MyDB_BufferManager :: getPinnedPage () {
	int file = 0;
	if (fileMap->find("tempPageFile") == fileMap->end()){
		file = open (tempFile.c_str(), O_RDWR | O_CREAT | O_FSYNC, 0666);
		fileMap->insert({"tempPageFile",file});
		fileToPageMap->insert({"tempPageFile",new unordered_map<long,Page*>});
		fileToAllPageMap->insert({"tempPageFile",new unordered_map<long,Page*>});
	}else{
		file =  fileMap->find("tempPageFile")->second;
	}
	Page* newPage = new Page(file);
	newPage->setManager(this);
	newPage->pinPage();
	int size = pageSize;
	int pos = findEmptySlot();
	if(pos==-1){ // Buffer is full, evict one
		pos = evict();
	}
	// insert in
	changeEmptySlot(pos,false);
	newPage->bufferNm = pos;
	newPage->toBuffer = bufferPool+pos*size;
	newPage->toLRU =getLRU()->addPage(newPage);
	return make_shared <MyDB_PageHandleBase> (newPage);
}

void MyDB_BufferManager :: unpin (MyDB_PageHandle unpinMe) {
	unpinMe->getPage()->unpinPage();
}

MyDB_BufferManager :: MyDB_BufferManager (size_t pageSize, size_t numPages, string tempFile) {
	this->pageSize = pageSize;
	this->numPages = numPages;
	this->tempFile = tempFile;
	size_t bufferSize = pageSize * numPages;
	bufferPool = (char*) malloc (bufferSize);
	emptySlots = new bool[numPages];
	for(int i=0;i<numPages;i++){
		emptySlots[i] = true;
	}
	LRU = new lruList();
	fileToPageMap = new unordered_map<string,unordered_map<long,Page*>*>();
	fileToAllPageMap = new unordered_map<string,unordered_map<long,Page*>*>();
	fileMap = new unordered_map<string,int> ();
	numPgs = 0;
	numPin = 0;
	tempPageStoreCount = 0;
}

MyDB_BufferManager :: ~MyDB_BufferManager () {
	Node * curr = LRU->head->next;
	int i=1;
	while(curr!=LRU->tail){
		lseek(curr->page->file,long(curr->page->pageLoc),SEEK_SET);
		write(curr->page->file,curr->page->toBuffer,pageSize);
		if(curr->page->handleNum==0){
			delete curr->page;
		}
		curr = curr->next;
		i++;
	}
	delete LRU;
	free(bufferPool);
	delete [] emptySlots;
	auto it = fileToPageMap->begin();
	while (it != fileToPageMap->end()) {
		delete it->second;
		it++;
	}
	it = fileToAllPageMap->begin();
	while (it != fileToAllPageMap->end()) {
		delete it->second;
		it++;
	}
	delete fileToPageMap;
	delete fileToAllPageMap;
	for (pair<string, int> element : *fileMap){
		close(element.second);
		if(element.first=="tempPageFile"){
			remove(tempFile.c_str());
		}
	}
	delete fileMap;
}

// PUBLIC HELPER FUNCTION for buffer manager----------------------------------
Page* MyDB_BufferManager::findInBuffer(string tableNm, long key) {
	if (fileToPageMap->find(tableNm) == fileToPageMap->end()){
		return nullptr;
	}else{
		unordered_map<long,Page*>* pageMap = fileToPageMap->find(tableNm)->second;
		if (pageMap->find(key) == pageMap->end()){
			return nullptr;
		}else{
			return pageMap->find(key)->second;
		}
	}
}

int MyDB_BufferManager::findEmptySlot() {
	for (int i=0;i<numPages;i++){
		if(emptySlots[i]){
			return i;
		}
	}
	return -1;
}

MyDB_BufferManager::lruList *MyDB_BufferManager::getLRU() {
	return LRU;
}

size_t MyDB_BufferManager::getPageSize() {
	return pageSize;
}

void MyDB_BufferManager::changeEmptySlot(int i, bool empty) {
	emptySlots[i] = empty;
}

char *MyDB_BufferManager::getBuffer() {
	return bufferPool;
}

unordered_map<string,unordered_map<long,Page*>*>* MyDB_BufferManager::getMap() {
	return fileToPageMap;
}

int MyDB_BufferManager::evict() {
	Node* curr = LRU->head->next;
	int evicted = -1;
	// find unpinned page to evict
	while(curr!=LRU->tail){
		if(curr->page->pinned){
			curr = curr->next;
		}else{ // find an unpinned page
			if(!curr->page->tempPage){ // not a temp page
				// if dirty, write back
				if(curr->page->dirty){
					lseek(curr->page->file,long(curr->page->pageLoc),SEEK_SET);
					write(curr->page->file,curr->page->toBuffer,pageSize);
				}
				evicted = curr->page->bufferNm;
				curr->page->bufferNm = -1;
				curr->page->toBuffer = nullptr;
				changeEmptySlot(evicted,true);
				long pageNum = curr->page->pgNum;
				LRU->removePage(curr);
				if(pageNum!=-1) {
					unordered_map<long,Page*>* pageMap =fileToPageMap->find(curr->page->tableNm)->second;
					pageMap->erase(pageNum);
				}
				if(curr->page->handleNum==0){
					delete curr->page;
				}
				delete curr;
				break;
			}else{ // temp page
				if(curr->page->pageLoc==-1){
					curr->page->pageLoc = tempPageStoreCount*pageSize;
					lseek(curr->page->file,long(pageSize)*(tempPageStoreCount),SEEK_SET);
					write(curr->page->file,curr->page->toBuffer,pageSize);
					tempPageStoreCount++;
				}else{
					lseek(curr->page->file,long(curr->page->pageLoc),SEEK_SET);
					write(curr->page->file,curr->page->toBuffer,pageSize);
				}
				evicted = curr->page->bufferNm;
				curr->page->toBuffer = nullptr;
				changeEmptySlot(evicted,true);
				LRU->removePage(curr);
				delete curr;
				break;
			}
		}
	}
	if(curr==LRU->tail){
		cout<<"evict() says: Error: Run out of buffer!"<<endl;
		exit(0);
	}
	return evicted;
}

// PUBLIC FUNCTION for LRU List class------------------------------------------
MyDB_BufferManager::lruList::lruList() {
	head = new Node();
	tail = new Node();
	head->sentinel = true;
	tail->sentinel = true;
	head->prev = nullptr;
	head->next = tail;
	tail->prev = head;
	tail->next = nullptr;
	pageCount = 0;
}

Node *MyDB_BufferManager::lruList::addPage(Page *page) {
	Node* curr = new Node();
	Node* last = tail->prev;
	last->next = curr;
	curr->prev = last;
	curr->next = tail;
	tail->prev = curr;
	curr->page = page;
	curr->sentinel = false;
	page->myBufferMng->numPgs++;
	if (page->pinned){
		page->myBufferMng->numPin++;
	}
	pageCount++;
	return curr;
}

void MyDB_BufferManager::lruList::removePage(Node * remove) {
	remove->prev->next = remove->next;
	remove->next->prev = remove->prev;
	remove->page->myBufferMng->numPgs--;
	//delete remove;
	pageCount--;
}

void MyDB_BufferManager::lruList::MRUPage(Node *move) {
	move->prev->next = move->next;
	move->next->prev = move->prev;
	move->prev = tail->prev;
	move->next = tail;
	tail->prev->next = move;
	tail->prev = move;
}

void MyDB_BufferManager::lruList::print() {
	cout<<"LRU sequence:"<<endl;
	cout<<"LRU (evict)->";
	Node* curr = head->next;
	while(curr!=tail){
		int temp = curr->page->pgNum;
		cout<<"page "<<temp;
		if (curr->page->pinned){
			cout<<"!";
		}
		cout<<"->";
		curr = curr->next;
	}
	cout<<"MRU (safe)"<<endl;
}

MyDB_BufferManager::lruList::~lruList() {
	Node * nextNode = head;
	while(head!=tail){
		nextNode = head->next;
		delete head;
		head = nextNode;
	}
	delete tail;
}



// End of LRU class-------------------------------------------------------------
// End of buffer manager class--------------------------------------------------

// PUBLIC FUNCTION for Page class----------------------------------------------
Page::Page(long i,int fileloc, string tableName,int locOfPage, bool pin) : pageLoc(locOfPage), file(fileloc), tableNm(tableName),
				toBuffer(nullptr),pgNum(i),bufferNm(-1),toLRU(nullptr), pinned(pin),
				 dirty(false), tempPage(false), handleNum(0),myBufferMng(nullptr){
}

Page::Page(int fileloc): pageLoc(-1), file(fileloc), tableNm("tempPageFile"),toBuffer(nullptr),pgNum(-1),
				bufferNm(-1), toLRU(nullptr), pinned(false), dirty(false), tempPage(true), handleNum(0),myBufferMng(nullptr){		
}

void Page::addHandleNum() {
	handleNum++;
}

void Page::delHandleNum() {
	// Unfinished
	handleNum--;
	if(handleNum==0){
		if(tempPage){ //temp page
			if(buffered()){
				myBufferMng->getLRU()->removePage(toLRU);
				delete toLRU;
				myBufferMng->changeEmptySlot(bufferNm,true);
			}
			delete this;
		}else{ //normal page
			if(!buffered()){	// not in the buffer
				delete this;
			}else{// in the buffer
				unpinPage();
				myBufferMng->numPin--;
			}
		}
	}
}

void Page::dirtyPage() {
	dirty = true;
}

bool Page::pinPage() {
	if(pinned){
		return false;
	}else {
		pinned = true;
		return true;
	}
}

bool Page::unpinPage() {
	if(!pinned){
		return false;
	}else{
		pinned = false;
		return true;
	}
}

void Page::setManager( MyDB_BufferManager* myBuffer) {
	myBufferMng = myBuffer;
}

void Page::setBuffer(char* locInBuffer) {
	toBuffer = locInBuffer;
	toLRU = myBufferMng->getLRU()->addPage(this);
	lseek(file,long(pageLoc),SEEK_SET);
	read(file,locInBuffer,myBufferMng->getPageSize());
}

void Page::unsetBuffer() {
	toBuffer = nullptr;
}

bool Page::buffered() {
	return toBuffer != nullptr;
}


Page::~Page() {
}
// End of Page class------------------------------------------------------------

#endif