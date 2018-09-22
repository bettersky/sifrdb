// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "util/coding.h"

namespace leveldb {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator           = 1,
  kLogNumber            = 2,
  kNextFileNumber       = 3,
  kLastSequence         = 4,
  kCompactPointer       = 5,
  //kDeletedFile        = 6,
  //kNewFile            = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,

  kLogicalFile           = 10,
  //kLevel              = 11,
  kPhysicalFile         = 12,
  kDeletedPhysicalFile  = 13,
  kDeletedLogicalFile   = 14
};

void VersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_physical_files_.clear();
  deleted_logical_files_.clear();
  new_logical_files_.clear();
}

void LogicalMetaData::AppendPhysicalFile(PhysicalMetaData &f) {
  if (physical_files.size() == 0) {
    smallest = f.smallest;
  }

  largest = f.largest;
  file_size += f.file_size;
  physical_files.push_back(f);
}

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode()); // InternalKey
  }

  //  Encode deleted physical files
  for (DeletedPhysicalFileSet::const_iterator iter = deleted_physical_files_.begin();
       iter != deleted_physical_files_.end(); ++iter) {
    PutVarint32(dst, kDeletedPhysicalFile);
    PutVarint64(dst, *iter);  // physical file number
	}

  // Encode deleted logical files
  for (DeletedLogicalFileSet::const_iterator iter = deleted_logical_files_.begin();
       iter != deleted_logical_files_.end(); ++iter) {
    PutVarint32(dst, kDeletedLogicalFile);
    PutVarint32(dst, iter->first);   // level
    PutVarint64(dst, iter->second);  // logical file number
	}

  // Encode new logical file
  for (size_t i = 0; i < new_logical_files_.size(); i++) {
    const LogicalMetaData& logical_f = new_logical_files_[i].second;
    int level = new_logical_files_[i].first;
    int sst_num = logical_f.physical_files.size();

    PutVarint32(dst, kLogicalFile);
    PutVarint32(dst, level);
    PutVarint32(dst, sst_num);

    PutVarint64(dst, logical_f.number);
    PutVarint64(dst, logical_f.file_size);
    PutLengthPrefixedSlice(dst, logical_f.smallest.Encode());
    PutLengthPrefixedSlice(dst, logical_f.largest.Encode());

    // For each logical file, encode all of its physical file.
    for (int j = 0; j < logical_f.physical_files.size(); j++) {
      const PhysicalMetaData& f = logical_f.physical_files[j];
      PutVarint32(dst, kPhysicalFile);
      PutVarint64(dst, f.number);
      PutVarint64(dst, f.file_size);
      PutLengthPrefixedSlice(dst, f.smallest.Encode());
      PutLengthPrefixedSlice(dst, f.largest.Encode());
    }
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

static bool GetLevel(Slice* input, int* level) {
  uint32_t v;
  if (GetVarint32(input, &v) &&
      v < config::kNumLevels) {
    *level = v;
    return true;
  } else {
    return false;
  }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  // Temporary storage for parsing
  uint64_t number;
  PhysicalMetaData physical_f;
  Slice str;
  InternalKey key;

  LogicalMetaData logical_f;
  int level;
  uint32_t sst_num;

  uint64_t logical_decoded_size;
  InternalKey logical_decoded_smallest;
  InternalKey logical_decoded_largest;
  // TODO: May cause memory leak
  const InternalKeyComparator *icmp_ = new InternalKeyComparator( (new Options())->comparator);

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:       // 1
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:        // 2
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:    // 9
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:   // 3
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:	    // 4
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kCompactPointer:
        if (GetLevel(&input, &level) &&
            GetInternalKey(&input, &key)) {
          compact_pointers_.push_back(std::make_pair(level, key));
        } else {
          msg = "compaction pointer";
        }
        break;

      // case kDeletedFile:
      //   if (GetLevel(&input, &level) &&
      //       GetVarint64(&input, &number)) {
      //     deleted_files_.insert(std::make_pair(level, number));
      //   } else {
      //     msg = "deleted file";
      //   }
      //   break;

      case kDeletedPhysicalFile:
          if (GetVarint64(&input, &number)) {
            deleted_physical_files_.insert(number);
          } else {
            msg = "deleted physical file";
          }
          break;

      case kDeletedLogicalFile:
          if (GetLevel(&input, &level) &&
              GetVarint64(&input, &number)) {
            deleted_logical_files_.insert(std::make_pair(level, number));
          } else {
            msg = "deleted logical file";
          }
          break;

      case kLogicalFile:        // 10
          if (GetLevel(&input, &level) &&
            GetVarint32(&input, &sst_num) &&
            GetVarint64(&input, &logical_f.number) &&
            GetVarint64(&input, &logical_decoded_size) &&
            GetInternalKey(&input, &logical_decoded_smallest) &&
            GetInternalKey(&input, &logical_decoded_largest)) {
          } else {
            msg = "logical file";
          }
          break;

          // Decode physical files for this logical file
        case kPhysicalFile:
          if (GetVarint64(&input, &physical_f.number) &&
              GetVarint64(&input, &physical_f.file_size) &&
              GetInternalKey(&input, &physical_f.smallest) &&
              GetInternalKey(&input, &physical_f.largest)) {
            logical_f.AppendPhysicalFile(physical_f);
            if (logical_f.physical_files.size() == sst_num) {
              if (logical_f.file_size != logical_decoded_size ||
                icmp_->Compare(logical_f.smallest, logical_decoded_smallest) != 0 ||
                icmp_->Compare(logical_f.largest, logical_decoded_largest) != 0) {
                msg = "physical file num";
              }
              new_logical_files_.push_back(std::make_pair(level, logical_f));
              logical_f.physical_files.clear();
              logical_f.file_size = 0;
            }
          } else {
            msg = "physical file";
          }
          break;

        default:
          msg = "unknown tag";
          break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("VersionEdit", msg);
  }
  return result;
}

std::string VersionEdit::DebugString() const {
  std::string r;
  r.append("VersionEdit {");
  if (has_comparator_) {
    r.append("\n  Comparator: ");
    r.append(comparator_);
  }
  if (has_log_number_) {
    r.append("\n  LogNumber: ");
    AppendNumberTo(&r, log_number_);
  }
  if (has_prev_log_number_) {
    r.append("\n  PrevLogNumber: ");
    AppendNumberTo(&r, prev_log_number_);
  }
  if (has_next_file_number_) {
    r.append("\n  NextFile: ");
    AppendNumberTo(&r, next_file_number_);
  }
  if (has_last_sequence_) {
    r.append("\n  LastSeq: ");
    AppendNumberTo(&r, last_sequence_);
  }
  for (size_t i = 0; i < compact_pointers_.size(); i++) {
    r.append("\n  CompactPointer: ");
    AppendNumberTo(&r, compact_pointers_[i].first);
    r.append(" ");
    r.append(compact_pointers_[i].second.DebugString());
  }
  for (DeletedPhysicalFileSet::const_iterator iter = deleted_physical_files_.begin();
       iter != deleted_physical_files_.end();
       ++iter) {
    r.append("\n  DeletePhysicalFile: ");
    AppendNumberTo(&r, *iter);
  }
  for (DeletedLogicalFileSet::const_iterator iter = deleted_logical_files_.begin();
       iter != deleted_logical_files_.end(); ++iter) {
    r.append("\n  DeleteLogicalFile: ");
    AppendNumberTo(&r, iter->first);
    r.append(" ");
    AppendNumberTo(&r, iter->second);
	}
  for (size_t i = 0; i < new_logical_files_.size(); i++) {
    const LogicalMetaData logical_f = new_logical_files_[i].second;
    int level = new_logical_files_[i].first;
		int sst_num = logical_f.physical_files.size();

    r.append("\n  NewLogicalFiles: ");
    AppendNumberTo(&r, level);
    r.append(" ");
    AppendNumberTo(&r, sst_num);
    AppendNumberTo(&r, logical_f.number);
    r.append(" ");
    AppendNumberTo(&r, logical_f.file_size);
    r.append(" ");
    r.append(logical_f.smallest.DebugString());
    r.append(" .. ");
    r.append(logical_f.largest.DebugString());

    for (size_t j = 0; j < logical_f.physical_files.size(); j++) {
      const PhysicalMetaData& f = logical_f.physical_files[j];
      r.append("\n  PhysicalFile: ");
      AppendNumberTo(&r, f.number);
      r.append(" ");
      AppendNumberTo(&r, f.file_size);
      r.append(" ");
      r.append(f.smallest.DebugString());
      r.append(" .. ");
      r.append(f.largest.DebugString());
    }
  }
  r.append("\n}\n");
  return r;
}

}  // namespace leveldb
