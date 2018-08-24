// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();
	  

	virtual int isNewSST(){
			//printf("TwoLevelIterator.cc, isNewSST\n");
			return new_sst_flag;
	}

	virtual Slice  currentSSTLargestKey(){
			//printf("TwoLevelIterator.cc, currentSSTLargestKey\n");

			return index_iter_.currentSSTLargestKey();

	}
	virtual Slice  currentSSTSmallestKey(){
			//printf("TwoLevelIterator.cc, currentSSTSmallestKey\n");

			return index_iter_.currentSSTSmallestKey();

	}
	virtual  Slice  nextSSTSmallestKey(){
			//printf("TwoLevelIterator.cc, nextSSTSmallestKey\n");

			return index_iter_.nextSSTSmallestKey();
	}
	virtual int next_sst(){
			//printf("TwoLevelIterator.cc, next_sst\n");
			//index_iter_.Valid();
			//printf("TwoLevelIterator.cc, next_sst, before calling index_iter_ next_sst============\n");
			//index_iter_.next_sst();
			index_iter_.next_sst();

			//index_iter_.Next();//there is an update in warpper.
			//index_iter_.Next();
			//int r=index_iter_.Valid();
			//index_iter_.Valid();
			//index_iter_.Valid();
			//printf("TwoLevelIterator.cc, next_sst, after called index_iter_ Valid %d\n",index_iter_.Valid());
			//exit(9);
			//printf("TwoLevelIterator.cc, next_sst,index_iter_.Valid()=%d\n",index_iter_.Valid());

			InitDataBlock();//now we have give a data iterator to the data_iter_ wrapper, however, the data_iter_
			if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();

			//printf("TwoLevelIterator.cc, next_sst, after called InitDataBlock, data_iter_.Valid()= %d\n",data_iter_.Valid());

			//SkipEmptyDataBlocksForward();
			//if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
			//printf("TwoLevelIterator.cc, next_sst, after called data_iter_.Valid()= %d\n",data_iter_.Valid());
	}
	
	virtual int get_sst_meta(const void **arg){
			//printf("TwoLevelIterator.cc, get_sst_meta\n");
			index_iter_.get_sst_meta(arg);
	}

	int new_sst_flag;
	Slice current_sst_largest_key;
	Slice next_sst_smallest_key;

  virtual bool Valid() const {
		//printf("two_level_iterator.cc, Valid, data_iter_=%p\n",data_iter_);
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  int SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  IteratorWrapper index_iter_;
  IteratorWrapper data_iter_; // May be NULL
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
		new_sst_flag=0;
}

TwoLevelIterator::~TwoLevelIterator() {
}




void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {

//I will know if this the last key.
	//fprintf(stderr,"two_level_iterator.cc, Next\n");
  assert(Valid());
  data_iter_.Next();//data_iter_ is also a two level iterator
	new_sst_flag=0;//Once the Next is called, new_sst_flag is marked as 0.
  SkipEmptyDataBlocksForward();//May mark the new_sst_flag to 1.
	//fprintf(stderr,"two_level_iterator.cc, next, end, new_sst_flag=%d\n",new_sst_flag);


  
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


int TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
	//printf("two_level_iterator.cc, move to the next data block\n");

    if (!index_iter_.Valid()) {//
      SetDataIterator(NULL);
      return 0;
    }
	
	//printf("two_level_iterator.cc, move to the next sst\n");
    index_iter_.Next();//move to the next SST
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
  return 1;
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
	//printf("two_level, InitDataBlock,111111111, begin,index_iter_.Valid()=%d\n",index_iter_.Valid());
	
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();
	//printf("two_level, InitDataBlock,33333333333, data_iter_.iter()=%p,handle.compare(data_block_handle_)=%d\n",data_iter_.iter(),handle.compare(data_block_handle_));
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
		//printf("two_level, InitDataBlock,44444444444444444 \n");
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
	  //printf("two_level, InitDataBlock,44444444444444444,data_iter_.iter()=%p ,iter.Valid()=%d\n",data_iter_.iter(),iter->Valid());
		new_sst_flag=1;//mark new sst
		//set current_sst_largest_key
		//set next_sst_smallest_key
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
