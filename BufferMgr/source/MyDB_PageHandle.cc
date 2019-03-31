// version 0124.1

#ifndef PAGE_HANDLE_C
#define PAGE_HANDLE_C

#include <memory>
#include <iostream>

#include "MyDB_PageHandle.h"
#include "MyDB_BufferManager.h"

void *MyDB_PageHandleBase :: getBytes () {
	if(toPage->buffered()){ // already in buffer
		MyDB_BufferManager* manager = toPage->myBufferMng;
		manager->getLRU()->MRUPage(toPage->toLRU);
		return (void*) (toPage->toBuffer);
	}else{ // insert page to buffer
		if(toPage->tempPage){ //temp page
			MyDB_BufferManager* manager = toPage->myBufferMng;
			int size = manager->getPageSize();
			int pos = manager->findEmptySlot();
			if(pos==-1){ // Buffer is full, evict one
				pos = toPage->myBufferMng->evict();
			}
			// insert in
			manager->changeEmptySlot(pos,false);
			toPage->bufferNm = pos;
			if(toPage->pageLoc!=-1){
				toPage->setBuffer(manager->getBuffer()+pos*size);
			}else{
				toPage->toBuffer = manager->getBuffer()+pos*size;
				toPage->toLRU = manager->getLRU()->addPage(toPage);
			}
			return (void*) (toPage->toBuffer);
		}else{ // normal page
			MyDB_BufferManager* manager = toPage->myBufferMng;
			int size = manager->getPageSize();
			int pos = manager->findEmptySlot();
			if(pos==-1){ // Buffer is full, evict one
				pos = toPage->myBufferMng->evict();
			}
			// insert in
			manager->changeEmptySlot(pos,false);
			unordered_map<long,Page*>* pageMap = manager->getMap()->find(toPage->tableNm)->second;
			pageMap->insert({toPage->pgNum,toPage});
			toPage->bufferNm = pos;
			toPage->setBuffer(manager->getBuffer()+pos*size);
			return (void*) (toPage->toBuffer);
		}
	}
}

void MyDB_PageHandleBase :: wroteBytes () {
	toPage->dirtyPage();
}

MyDB_PageHandleBase :: ~MyDB_PageHandleBase () {
	toPage->delHandleNum();
}

MyDB_PageHandleBase::MyDB_PageHandleBase(Page *page) {
	toPage = page;
	toPage->addHandleNum();
}

Page *MyDB_PageHandleBase::getPage() {
	return toPage;
}

#endif

