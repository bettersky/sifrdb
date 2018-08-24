// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

#include "stdlib.h"

//extern int flag,flag1;

//extern double durations[];
namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
	//printf("table cache.cc, find table out of cache, from page cache or disk, file_number=%d, file_size=%d MB\n",file_number,file_size/1024/1024);
	//static int x=0;
	//x++;
	//if(x==18){
		//exit(9);
	//}
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
//printf("tableache, begin, file number=%d\n",file_number);
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
//#define s_to_ns 1000000000  


//struct timespec findtable_seek_begin, findtable_seek_end;
//clock_gettime(CLOCK_MONOTONIC,&findtable_seek_begin);   
  Status s = FindTable(file_number, file_size, &handle);//find sst index in cache
//clock_gettime(CLOCK_MONOTONIC,&findtable_seek_end); 
//durations[7]+=( (int)findtable_seek_end.tv_sec+((double)findtable_seek_end.tv_nsec)/s_to_ns ) - ( (int)findtable_seek_begin.tv_sec+((double)findtable_seek_begin.tv_nsec)/s_to_ns );

  //printf("table_cache.cc, file_number=%d,file_size=%f MB----------------\n",file_number, (double)file_size/1024/1024);
  //flag1=0;
  //int i=0;
  //if( (file_size/1024/1024)>i && (file_size/1024/1024)<i*10){
		//exit(9);
		//flag1=1;
		//printf("table_cache.cc, file_number=%d,file_size=%f MB----------------\n",file_number, (double)file_size/1024/1024);
  //}
 
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
	//printf("table_cache.cc,before internalGet file_number=%d,file_size=%d MB\n",file_number, file_size/1024/1024);
//struct timespec InternalGet_begin, InternalGet_end;
//clock_gettime(CLOCK_MONOTONIC,&InternalGet_begin); 
    s = t->InternalGet(options, k, arg, saver);

//clock_gettime(CLOCK_MONOTONIC,&InternalGet_end); 
//durations[8]+=( (int)InternalGet_end.tv_sec+((double)InternalGet_end.tv_nsec)/s_to_ns ) - ( (int)InternalGet_begin.tv_sec+((double)InternalGet_begin.tv_nsec)/s_to_ns );

	//printf("table_cache.cc,after internalGet file_number=%d,file_size=%d MB\n",file_number, file_size/1024/1024);
    cache_->Release(handle);
  }
  

  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
