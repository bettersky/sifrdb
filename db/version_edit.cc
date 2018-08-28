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
  //kDeletedFile          = 6,
  //kNewFile              = 7,
  // 8 was used for large value refs
  kPrevLogNumber        = 9,

	kLogicalFile			=10,
	//kLevel					=11,
	kPhysicalFile			=12,
	kDeletedPhysicalFile			=13,
	kDeletedLogicalFile			=14
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
  //new_files_.clear();
  //new_logical_file=LogicalMetaData();
  new_logical_files_.clear();
}

void LogicalMetaData::AppendPhysicalFile(PhysicalMetaData &f) {
		if (physical_files.size() == 0) {
			smallest=f.smallest;
		} else {
			const InternalKeyComparator *icmp_=new InternalKeyComparator( (new Options())->comparator);//May cause memory leak
			if(icmp_->Compare(f.smallest, smallest)<=0 ){
				fprintf(stderr, "version_edit.cc, AppendPhysicalFile. err, \n");
				exit(9);
			}		
		}
		largest = f.largest;

		file_size += f.file_size; 
		physical_files.push_back(f);
}

void VersionEdit::EncodeTo(std::string* dst) const {

		
	//printf("version_edit.cc, EncodeTo, begin, dst=%s\n",dst->c_str());
  if (has_comparator_) {
	//printf("version_edit.cc, EncodeTo, has_comparator_\n");
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
		//printf("version_edit.cc, EncodeTo, has_log_number_\n");

    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
		//printf("version_edit.cc, EncodeTo, has_prev_log_number_\n");

    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
		//rintf("version_edit.cc, EncodeTo, has_next_file_number_\n");

    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
		//printf("version_edit.cc, EncodeTo, has_last_sequence_\n");

    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }

  for (size_t i = 0; i < compact_pointers_.size(); i++) {
		//printf("version_edit.cc, EncodeTo,  compact_pointers_.size()=%d\n", compact_pointers_.size());

    PutVarint32(dst, kCompactPointer);
    PutVarint32(dst, compact_pointers_[i].first);  // level
    PutLengthPrefixedSlice(dst, compact_pointers_[i].second.Encode());// InternalKey
  }

  // for (DeletedFileSet::const_iterator iter = deleted_files_.begin();
       // iter != deleted_files_.end();
       // ++iter) {
    // PutVarint32(dst, kDeletedFile);
    // PutVarint32(dst, iter->first);   // level
    // PutVarint64(dst, iter->second);  // file number
  // }
 //*** encode deleted physical files  
   for (DeletedPhysicalFileSet::const_iterator iter = deleted_physical_files_.begin();iter != deleted_physical_files_.end();++iter) {
		//printf("version_edit.cc, EncodeTo, DeletedPhysicalFileSet\n");
		PutVarint32(dst, kDeletedPhysicalFile);
		//PutVarint32(dst, iter->first);   // level  //no level
		PutVarint64(dst, *iter);  // physical file number
	}
	
//*** encode deleted logical files     
   for (DeletedLogicalFileSet::const_iterator iter = deleted_logical_files_.begin();iter != deleted_logical_files_.end();++iter) {
	//printf("version_edit.cc, EncodeTo, DeletedLogicalFileSet\n");
		PutVarint32(dst, kDeletedLogicalFile);
		PutVarint32(dst, iter->first);   // level  
		PutVarint64(dst, iter->second);  // logical file number
	}
	
/************tomb*******************************************************************************************/
  // for (size_t i = 0; i < new_files_.size(); i++) {
    // const FileMetaData& f = new_files_[i].second;
    // PutVarint32(dst, kNewFile);
    // PutVarint32(dst, new_files_[i].first);  // level
    // PutVarint64(dst, f.number);
    // PutVarint64(dst, f.file_size);
    // PutLengthPrefixedSlice(dst, f.smallest.Encode());
    // PutLengthPrefixedSlice(dst, f.largest.Encode());
  // }
/************tomb*******************************************************************************************/

	for (size_t i = 0; i < new_logical_files_.size(); i++) {
		const LogicalMetaData& logical_f = new_logical_files_[i].second;
		int level = new_logical_files_[i].first;
		int sst_num= logical_f.physical_files.size();
		//encode this logical file
		PutVarint32(dst, kLogicalFile);
		PutVarint32(dst, level);
		PutVarint32(dst, sst_num);
		
		PutVarint64(dst, logical_f.number);
		PutVarint64(dst, logical_f.file_size);
		PutLengthPrefixedSlice(dst, logical_f.smallest.Encode());
		PutLengthPrefixedSlice(dst, logical_f.largest.Encode());
		
		//for each logcial file, encode all of its phsyical ssts.
		for(int j=0;j< logical_f.physical_files.size();j++){
			const PhysicalMetaData& f = logical_f.physical_files[j];
			
			PutVarint32(dst, kPhysicalFile);
			PutVarint64(dst, f.number);
			PutVarint64(dst, f.file_size);
			PutLengthPrefixedSlice(dst, f.smallest.Encode());
			PutLengthPrefixedSlice(dst, f.largest.Encode());
		
		}
		
		
	
	}
/************tomb2*******************************************************************************************/

	// if( new_logical_file.physical_files.size()>0){
		// //encode the logical tree number
		 // PutVarint32(dst, kLogicalFile);
		// //encode the level
		// PutVarint64(dst, level_of_new_logical_file);//2017.6.7---this may be a bug, should be PutVarint32

		// //endcode the logical tree meta
		// PutVarint64(dst, new_logical_file.number);
		// PutVarint64(dst, new_logical_file.file_size);
		// PutLengthPrefixedSlice(dst, new_logical_file.smallest.Encode());
		// PutLengthPrefixedSlice(dst, new_logical_file.largest.Encode());
	// }


	// //endcode the physical files
	 // for (size_t i = 0; i < new_logical_file.physical_files.size(); i++) {
				// //printf("version_edit.cc, EncodeTo, new_logical_file\n");

		// const PhysicalMetaData& f = new_logical_file.physical_files[i];
		// PutVarint32(dst, kPhysicalFile);
		// //PutVarint32(dst, new_logical_file.physical_files[i].first);  // level
		// PutVarint64(dst, f.number);
		// PutVarint64(dst, f.file_size);
		// PutLengthPrefixedSlice(dst, f.smallest.Encode());
		// PutLengthPrefixedSlice(dst, f.largest.Encode());
	// }
/************tomb2*******************************************************************************************/

	//printf("version_edit.cc, EncodeTo, end, dst=%s\n",dst->c_str());
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
  PhysicalMetaData physical_f;//*************************
  Slice str;
  InternalKey key;
	
	//LogicalMetaData logical_f;//**************************
	LogicalMetaData *logical_f_pointer;
	int level;
	uint32_t sst_num;
	
	
	uint64_t logical_decoded_size;
	InternalKey logical_decoded_smallest;
	InternalKey logical_decoded_largest;
	

  while (msg == NULL && GetVarint32(&input, &tag)) {
	//printf("version_edit.cc, DecodeFrom, tag=%d\n",tag);
    switch (tag) {
      case kComparator:    //1
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:  //2
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:   //9
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "previous log number";
        }
        break;

      case kNextFileNumber:   //3
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:	//4
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
        // if (GetLevel(&input, &level) &&
            // GetVarint64(&input, &number)) {
          // deleted_files_.insert(std::make_pair(level, number));
        // } else {
          // msg = "deleted file";
        // }
        // break;
		
	case kDeletedPhysicalFile:
        if (GetVarint64(&input, &number)) {
          deleted_physical_files_.insert(number);
        } else {
          msg = "deleted physical file";
        }
        break;
	case kDeletedLogicalFile:
        if (GetLevel(&input, &level) &&GetVarint64(&input, &number)) {
          deleted_logical_files_.insert(std::make_pair(level, number));
        } else {
          msg = "deleted logical file";
        }
        break;
	//********get the logical files
		case kLogicalFile:  //10
			logical_f_pointer = new LogicalMetaData();
			
			//logical_f_pointer->physical_files.clear();
			if(logical_f_pointer->physical_files.size()>0){
				fprintf(stderr,"version_edit.cc, decode, err, exit\n");
				exit(9);
			}
			//printf("ersion_edit.cc, DecodeFrom, kLogicalFile,begin, logical_f_pointer->file_size=%d\n",logical_f_pointer->file_size);
			
			
			GetLevel(&input, &level);
			//printf("version_edit.cc, DecodeFrom,kLogicalFile, level=%d\n",level);
			GetVarint32(&input, &sst_num);
			//printf("version_edit.cc, DecodeFrom,kLogicalFile,sst_num=%d\n",sst_num);
			
			GetVarint64(&input, &logical_f_pointer->number);
			//printf("version_edit.cc, DecodeFrom,kLogicalFile,logical_f_pointer->number=%lld\n",logical_f_pointer->number);
			
			//GetVarint64(&input, &logical_f_pointer->file_size);//logical_decoded_size
			GetVarint64(&input, &logical_decoded_size);//logical_decoded_size
			//printf("version_edit.cc, DecodeFrom, kLogicalFile,logical_decoded_size=%lld\n",logical_decoded_size);
			
			GetInternalKey(&input, &logical_decoded_smallest);
			GetInternalKey(&input, &logical_decoded_largest);	

			//new_logical_files_.push_back(std::make_pair(level, logical_f));
			break;
	//***********get the physical files for this logical file
		case kPhysicalFile:
            GetVarint64(&input, &physical_f.number) ;
            GetVarint64(&input, &physical_f.file_size);
            GetInternalKey(&input, &physical_f.smallest);
            GetInternalKey(&input, &physical_f.largest);
			
			logical_f_pointer->AppendPhysicalFile(physical_f);//this is also a bug, should use AppendPhysicalFile			
			
			//printf("version_edit.cc, DecodeFrom,kPhysicalFile, after AppendPhysicalFile\n");

			//exit(9);
			if(logical_f_pointer->physical_files.size() == sst_num){

				const InternalKeyComparator *icmp_=new InternalKeyComparator( (new Options())->comparator);//May cause memory leak
				if(logical_f_pointer->file_size != logical_decoded_size||//Verify the two decoded logical datas are consistent.
					icmp_->Compare(logical_f_pointer->smallest, logical_decoded_smallest)!=0||
					icmp_->Compare(logical_f_pointer->largest, logical_decoded_largest)!=0){
					fprintf(stderr,"version_edit.cc, DecodeFrom,kPhysicalFile,err,logical_f_pointer->file_size!=logical_decoded_size=%d\n",logical_f_pointer->file_size!=logical_decoded_size);
					fprintf(stderr,"version_edit.cc, DecodeFrom,kPhysicalFile,err,icmp_->Compare(logical_f_pointer->smallest, logical_decoded_smallest)=%d\n",icmp_->Compare(logical_f_pointer->smallest, logical_decoded_smallest));
					fprintf(stderr,"version_edit.cc, DecodeFrom,kPhysicalFile,err,icmp_->Compare(logical_f_pointer->largest, logical_decoded_largest)=%d\n",icmp_->Compare(logical_f_pointer->largest, logical_decoded_largest));
					
					
					exit(9);
				}
				new_logical_files_.push_back(std::make_pair(level, *logical_f_pointer));
				delete logical_f_pointer;
				logical_f_pointer=NULL;
				//logical_f_pointer->physical_files.clear();				
			}
			break;

			
/************tomb*******************************************************************************************/

      // case kNewFile:
        // if (GetLevel(&input, &level) &&
            // GetVarint64(&input, &f.number) &&
            // GetVarint64(&input, &f.file_size) &&
            // GetInternalKey(&input, &f.smallest) &&
            // GetInternalKey(&input, &f.largest)) {
          // new_files_.push_back(std::make_pair(level, f));
        // } else {
          // msg = "new-file entry";
        // }
        // break;
/************tomb*******************************************************************************************/

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

		printf("version_edit.cc, DebugString");
		exit(9);
  // std::string r;
  // r.append("VersionEdit {");
  // if (has_comparator_) {
    // r.append("\n  Comparator: ");
    // r.append(comparator_);
  // }
  // if (has_log_number_) {
    // r.append("\n  LogNumber: ");
    // AppendNumberTo(&r, log_number_);
  // }
  // if (has_prev_log_number_) {
    // r.append("\n  PrevLogNumber: ");
    // AppendNumberTo(&r, prev_log_number_);
  // }
  // if (has_next_file_number_) {
    // r.append("\n  NextFile: ");
    // AppendNumberTo(&r, next_file_number_);
  // }
  // if (has_last_sequence_) {
    // r.append("\n  LastSeq: ");
    // AppendNumberTo(&r, last_sequence_);
  // }
  // for (size_t i = 0; i < compact_pointers_.size(); i++) {
    // r.append("\n  CompactPointer: ");
    // AppendNumberTo(&r, compact_pointers_[i].first);
    // r.append(" ");
    // r.append(compact_pointers_[i].second.DebugString());
  // }
// /************tomb*******************************************************************************************/
  
  // // for (DeletedFileSet::const_iterator iter = deleted_files_.begin();iter != deleted_files_.end();++iter) {
    // // r.append("\n  DeleteFile: ");
    // // AppendNumberTo(&r, iter->first);
    // // r.append(" ");
    // // AppendNumberTo(&r, iter->second);
  // // }
// /************tomb*******************************************************************************************/
  
  // for (DeletedLogicalFileSet::const_iterator iter = deleted_logical_files_.begin();iter != deleted_logical_files_.end();++iter) {
    // r.append("\n  DeleteLogicalFile: ");
    // AppendNumberTo(&r, iter->first);
    // r.append(" ");
    // AppendNumberTo(&r, iter->second);
  // }
  // for (DeletedPhysicalFileSet::const_iterator iter = deleted_physical_files_.begin();iter != deleted_physical_files_.end();++iter) {
    // r.append("\n  DeletePhysicalFile: ");
    // //AppendNumberTo(&r, iter->first);
    // //r.append(" ");
    // AppendNumberTo(&r, *iter);
  // }
  // //*****************here we should append the logical file, but as this is a debug information, we may do it later.
  
  
  // //*****************
  
  // // for (size_t i = 0; i <new_logical_file.physical_files.size(); i++) {
    // // const PhysicalMetaData& f = new_logical_file.physical_files[i];
    // // r.append("\n  AddPhysicalFile: ");
    // // //AppendNumberTo(&r, new_files_[i].first);
    // // //r.append(" ");
    // // AppendNumberTo(&r, f.number);
    // // r.append(" ");
    // // AppendNumberTo(&r, f.file_size);
    // // r.append(" ");
    // // r.append(f.smallest.DebugString());
    // // r.append(" .. ");
    // // r.append(f.largest.DebugString());
  // // }
  // r.append("\n}\n");
  // return r;
}

}  // namespace leveldb
