// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
#define STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_

#include "leveldb/iterator.h"
#include "leveldb/slice.h"

namespace leveldb {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
 public:
  IteratorWrapper(): iter_(NULL), valid_(false) { }
  explicit IteratorWrapper(Iterator* iter): iter_(NULL) {
    Set(iter);
  }
  ~IteratorWrapper() { delete iter_; }
  Iterator* iter() const { return iter_; }

  int flag;
  // Takes ownership of "iter" and will delete it when destroyed, or
  // when Set() is invoked again.

	bool IsNewSSTTable() {
		return iter_->IsNewSSTTable();
	}

  Slice currentSSTLargestKey() {
    return iter_->currentSSTLargestKey();
	}
  
  Slice currentSSTSmallestKey(){
    return iter_->currentSSTSmallestKey();
	}
	
  Slice nextSSTSmallestKey(){
		return iter_->nextSSTSmallestKey();
	}

	void NextSSTTable();
	
  void Set(Iterator* iter) ;

  // Iterator interface methods
  bool Valid() const ;
  
  Slice key() const   ;
  Slice value() const       { assert(Valid()); return iter_->value(); }
  // Methods below require iter() != NULL
  Status status() const     { assert(iter_); return iter_->status(); }
  void Next() ;
  void Prev()               { assert(iter_); iter_->Prev();        Update(); }
  void Seek(const Slice& k) { assert(iter_); iter_->Seek(k);       Update(); }
  void SeekToFirst()        { assert(iter_); iter_->SeekToFirst(); Update(); }
  void SeekToLast()         { assert(iter_); iter_->SeekToLast();  Update(); }
  PhysicalMetaData* GetSSTTableMeta() { assert(iter_); iter_->GetSSTTableMeta(); }
 private:
  void Update();

  Iterator* iter_;
  bool valid_;
  Slice key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_ITERATOR_WRAPPER_H_
