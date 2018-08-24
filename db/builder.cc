// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/filename.h"
#include "db/dbformat.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

#include <stdlib.h>
namespace leveldb {

Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  PhysicalMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  //fname=fname+"0";
  //printf("builder.cc, BuildTable, fname=%s\n",fname.c_str());
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);
      builder->Add(key, iter->value());
    }
	//exit(9);
    // Finish and check for builder errors
    if (s.ok()) {
      s = builder->Finish();
      if (s.ok()) {
        meta->file_size = builder->FileSize();
        assert(meta->file_size > 0);
      }
    } else {
      builder->Abandon();
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
	 //printf("builder.cc, BuildTable, after sync\n");
	//exit(9);
    delete file;
    file = NULL;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(),
                                              meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }
  //printf("build, s.ok=%d\n\n",s.ok());
 //exit(9);

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }
 //printf("builder.cc, BuildTable, before meta\n");
//exit(9);
  if (s.ok() && meta->file_size > 0) {
	 //printf("builder.cc, BuildTable, keep file,s.ok()=%d\n",s.ok());
    // Keep it
  } else {
		 printf("builder.cc, BuildTable, delete file, %s\n",fname.c_str());
		
    env->DeleteFile(fname);
  }
  //exit(9);
  return s;
}

}  // namespace leveldb
