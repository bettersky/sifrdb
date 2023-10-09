// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>
#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct PhysicalMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  PhysicalMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

struct LogicalMetaData {
  int refs;
  int allowed_seeks;
  uint64_t number;	
  uint64_t file_size;
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  std::vector<PhysicalMetaData> physical_files;

  void AppendPhysicalFile(PhysicalMetaData &f);

  LogicalMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {
    physical_files.clear();
  }
};

class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() { }

  void Clear();

  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
	void AddLogicalFile(int level, const LogicalMetaData &logical_f) {
		new_logical_files_.push_back(std::make_pair(level, logical_f));
	}

  // Delete the specified "file" from the specified "level".
  void DeletePhysicalFile(uint64_t file) {        // physical file number
    deleted_physical_files_.insert(file);
  }
  
  void DeleteLogicalFile(int level, uint64_t file) {  // physical file number
    deleted_logical_files_.insert(std::make_pair(level, file));
  }
	
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set< uint64_t> DeletedPhysicalFileSet;
  typedef std::set< std::pair<int, uint64_t> > DeletedLogicalFileSet;

  std::string comparator_;
  uint64_t log_number_;   // log file is physical
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector< std::pair<int, InternalKey> > compact_pointers_;
  
  DeletedPhysicalFileSet deleted_physical_files_;
  DeletedLogicalFileSet deleted_logical_files_;
  std::vector<std::pair<int, LogicalMetaData>> new_logical_files_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
