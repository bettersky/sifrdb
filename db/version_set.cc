// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <stdio.h>
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

#include "port/port.h"
#include <unistd.h>

#include "stdlib.h"	

#define s_to_ns 1000000000  
extern double durations[];

extern int growth_factor;

extern int read_method;

extern int read_thread;



namespace leveldb {

static const uint64_t kTargetFileSize = 2 * 1048576;

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static const int64_t kMaxGrandParentOverlapBytes = 1000*1000 * kTargetFileSize;

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static const int64_t kExpandedCompactionByteSizeLimit = 2500*1000 * kTargetFileSize;

static double MaxBytesForLevel(int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.
  double result = 1000*1000 * 1048576.0;  // Result for both level-0 and level-1
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(int level) {
	return kTargetFileSize;
  //return kTargetFileSize*(level+2);  // We could vary per level to reduce number of files?
}

static int64_t TotalFileSize(const std::vector<LogicalMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < logical_files_[level].size(); i++) {
      LogicalMetaData* f = logical_files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

int FindFile(const InternalKeyComparator& icmp,
			 const LogicalMetaData& logical_file,
             const Slice& key) {//find the physical file that possiblely includes the key,  in the given logical file 
  uint32_t left = 0;
  uint32_t right = logical_file.physical_files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const PhysicalMetaData* physical_f = &(logical_file.physical_files[mid]);
    if (icmp.InternalKeyComparator::Compare(physical_f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const LogicalMetaData* f) {
  // NULL user_key occurs before all keys and is therefore never after *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const LogicalMetaData* f) {
  // NULL user_key occurs after all keys and is therefore never before *f
  return (user_key != NULL &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

// bool SomeFileOverlapsRange(
    // const InternalKeyComparator& icmp,
    // bool disjoint_sorted_files,
    // const std::vector<LogicalMetaData*>& files,
    // const Slice* smallest_user_key,
    // const Slice* largest_user_key) {
  // const Comparator* ucmp = icmp.user_comparator();
  // if (!disjoint_sorted_files) {
    // // Need to check against all files
    // for (size_t i = 0; i < files.size(); i++) {
      // const LogicalMetaData* f = files[i];
      // if (AfterFile(ucmp, smallest_user_key, f) ||
          // BeforeFile(ucmp, largest_user_key, f)) {
        // // No overlap
      // } else {
        // return true;  // Overlap
      // }
    // }
    // return false;
  // }

  // // Binary search over file list
  // uint32_t index = 0;
  // if (smallest_user_key != NULL) {
    // // Find the earliest possible internal key for smallest_user_key
    // InternalKey small(*smallest_user_key, kMaxSequenceNumber,kValueTypeForSeek);
    // index = FindFile(icmp, files, small.Encode());
  // }

  // if (index >= files.size()) {
    // // beginning of range is after all files, so no overlap.
    // return false;
  // }

  // return !BeforeFile(ucmp, largest_user_key, files[index]);
// }

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.

class Version::LogicalSSTNumIterator : public Iterator {
 public:
  LogicalSSTNumIterator(const InternalKeyComparator& icmp,
                       const LogicalMetaData* flist)
      : icmp_(icmp),
        flist_(flist),
        index_(flist->physical_files.size()) {        // Marks as invalid
  }

	virtual Slice currentSSTLargestKey(){
		//printf("versionset.cc, currentSSTLargestKey,index_=%d\n",index_);
		return flist_->physical_files [index_].largest.Encode() ;


	}
	virtual Slice currentSSTSmallestKey(){
		//printf("versionset.cc, currentSSTSmallestKey,index_=%d\n",index_);
		
		return flist_->physical_files [index_].smallest.Encode() ;


	}

	virtual Slice nextSSTSmallestKey(){ 
		printf("versionset.cc, nextSSTSmallestKey\n");
	}
	
	virtual int get_sst_meta(const void **arg){
		//printf("versionset.cc, get_sst_meta\n");

		*arg=&flist_->physical_files[index_];
	}
	virtual int next_sst(){ 
		//printf("versionset.cc, next_sst, index_=%d, flist_->physical_files.size()=%d\n",index_,flist_->physical_files.size());
    assert(Valid());
		index_++;
		//printf("versionset.cc, next_sst, index_=%d, Valid=%d\n",index_, Valid());
		return 0;
	}	
	
  virtual bool Valid() const {
	//printf("versionset.cc, Valid, aaaa index_=%d\n",index_);
    return index_ < flist_->physical_files.size();
  }
  virtual void Seek(const Slice& target) {
    index_ = FindFile(icmp_, *flist_, target);
  }
  virtual void SeekToFirst() { index_ = 0; }
  virtual void SeekToLast() {
    index_ = flist_->physical_files.empty() ? 0 : flist_->physical_files.size() - 1;
  }
  virtual void Next() {
	//printf("versionset.cc, Next,index_=%d\n",index_);
    assert(Valid());
    index_++;
  }
  virtual void Prev() {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->physical_files.size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const {
    assert(Valid());
    return (flist_->physical_files)[index_].largest.Encode();
  }
  Slice value() const {
    assert(Valid());
    EncodeFixed64(value_buf_, (flist_->physical_files)[index_].number);
    EncodeFixed64(value_buf_+8, (flist_->physical_files)[index_].file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  //const std::vector<PhysicalMetaData*>* const flist_;
  const LogicalMetaData *flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};


class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp, const LogicalMetaData* logical_f)
		: icmp_(icmp),
        logical_f_(logical_f)
        //index_(flist->size()) 
	        // Marks as invalid
	{
		printf("version_set.cc, LevelFileNumIterator!,exit!\n");
		exit(9);
	}
  // virtual bool Valid() const {
    // return index_ < logical_f_->physical_files.size();
  // }
  // virtual void Seek(const Slice& target) {
    // index_ = FindFile(icmp_, *logical_f_, target);
  // }
  // virtual void SeekToFirst() { index_ = 0; }
  // virtual void SeekToLast() {
    // index_ = flist_->empty() ? 0 : flist_->size() - 1;
  // }
  // virtual void Next() {
    // assert(Valid());
    // index_++;
  // }
  // virtual void Prev() {
    // assert(Valid());
    // if (index_ == 0) {
      // index_ = flist_->size();  // Marks as invalid
    // } else {
      // index_--;
    // }
  // }
  // Slice key() const {
    // assert(Valid());
    // return (*flist_)[index_]->largest.Encode();
  // }
  // Slice value() const {
    // assert(Valid());
    // EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    // EncodeFixed64(value_buf_+8, (*flist_)[index_]->file_size);
    // return Slice(value_buf_, sizeof(value_buf_));
  // }
  // virtual Status status() const { return Status::OK(); }
 private:
  const InternalKeyComparator icmp_;
  //const std::vector<FileMetaData*>* const flist_;
	const LogicalMetaData* const logical_f_;
  uint32_t index_;

  // // Backing store for value().  Holds the file number and size.
  // mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg,
                                 const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options,
                              DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,int level) const {
	printf("version_set.cc exit\n");
	exit(9);
  // return NewTwoLevelIterator(
      // new LevelFileNumIterator(vset_->icmp_, &logical_files_[level]),
      // &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {


	//printf("version_set.cc,AddIterators, exit\n");
	//exit(9);

	for(int level=0;level<config::kNumLevels;level++){
		for(int i=0;i<logical_files_[level].size();i++){
			iters->push_back(
							NewTwoLevelIterator(new LogicalSSTNumIterator(vset_->icmp_, logical_files_[level][i]),&GetFileIterator, vset_->table_cache_, options)
							);
		}
	}
  // Merge all level zero files together since they may overlap
  // for (size_t i = 0; i < files_[0].size(); i++) {
    // iters->push_back(
        // vset_->table_cache_->NewIterator(
            // options, files_[0][i]->number, files_[0][i]->file_size));
  // }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  // for (int level = 1; level < config::kNumLevels; level++) {
    // if (!files_[level].empty()) {
      // iters->push_back(NewConcatenatingIterator(options, level));
    // }
  // }
}

// Callback from TableCache::Get()


static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  //printf("SaveValue,............................\n");
  if (!ParseInternalKey(ikey, &parsed_key)) {
		// printf("SaveValue,.corrupt**********************\n");
    s->state = kCorrupt;
  } else {
		 //printf("SaveValue, not corrupt,parsed_key.user_key=%s,s->user_key=%s |||||||||||||||||||||||)\n",parsed_key.user_key.data(),s->user_key.data());
		 //printf("cmp_result=%d..........................\n", s->ucmp->Compare(parsed_key.user_key, s->user_key));
		 		//printf("parsed_key.user_key size=%d, s->user_key size=%d\n",parsed_key.user_key.size(), s->user_key.size() );

		  //printf("cmp_result222222=%d..........................\n", s->ucmp->Compare(Slice("dd"),Slice("dd")));
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
			 //printf("SaveValue, in if888888888888888888\n");
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
		//printf("SaveValue, found+++++++++++++++++++++++\n");
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(LogicalMetaData* a, LogicalMetaData* b) {
  return a->number > b->number;
}

// void Version::ForEachOverlapping(Slice user_key, Slice internal_key,
                                 // void* arg,
                                 // bool (*func)(void*, int, FileMetaData*)) {
  // // TODO(sanjay): Change Version::Get() to use this function.
  // const Comparator* ucmp = vset_->icmp_.user_comparator();

  // // Search level-0 in order from newest to oldest.
  // std::vector<FileMetaData*> tmp;
  // tmp.reserve(files_[0].size());
  // for (uint32_t i = 0; i < files_[0].size(); i++) {
    // FileMetaData* f = files_[0][i];
    // if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        // ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      // tmp.push_back(f);
    // }
  // }
  // if (!tmp.empty()) {
    // std::sort(tmp.begin(), tmp.end(), NewestFirst);
    // for (uint32_t i = 0; i < tmp.size(); i++) {
      // if (!(*func)(arg, 0, tmp[i])) {
        // return;
      // }
    // }
  // }

  // // Search other levels.
  // for (int level = 1; level < config::kNumLevels; level++) {
    // size_t num_files = files_[level].size();
    // if (num_files == 0) continue;

    // // Binary search to find earliest index whose largest key >= internal_key.
    // uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    // if (index < num_files) {
      // FileMetaData* f = files_[level][index];
      // if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // // All of "f" is past any data for user_key
      // } else {
        // if (!(*func)(arg, level, f)) {
          // return;
        // }
      // }
    // }
  // }
// }

void Version::Create_scan_threads(int thread_num){//thread_num is TN in version_set.h
	for(int i=0;i<thread_num;i++){
		pthread_create(&pth_scan[i], NULL,  &Version::Scan_Back_starter, this);
	}
}

void Version::Fores_thread_create(int thread_num){//thread_num is TN in version_set.h
	for(int i=0;i<thread_num;i++){
		pthread_create(&pth_forest[i], NULL,  &Version::Back_starter, this);
	}
}

struct timespec search_begin, search_stage;

int scan_th_counter=0;

void  Version::Scan_back(){
	mutex_for_scan.Lock();
	uint32_t th_id=scan_th_counter++;
	mutex_for_scan.Unlock();
	printf("versionset.cc, Scan_back begin,scan_right=%p, th_id=%d\n",scan_right,th_id);
	while(true){
//*********pre concurrent search begin**********			

			mutex_for_scan.Lock();
			int current_id;//determined by the search id of the selected logical file

			while(1){

				int found_logical_file_to_scan=0;								
				//get an SST to scan begin
					if(scan_right==NULL||scan_right->should_pop==1 || scan_right->logical_file_counter >= scan_logical_file_total){//no item to search, wait
						//printf("versionset.cc, item is null\n");
					}
					else if(scan_right->logical_file_counter < scan_logical_file_total){//found an sst to search
						found_logical_file_to_scan=1; 						
					}
					else{
						fprintf(stderr,"version_set, Forest_back,err\n");
						exit(9);
										
					}
				//get an SST to search end
				
				if(0==found_logical_file_to_scan){//go to wait for the next request
					scan_thread_idle++;
					//printf("version_set, scan_back, enter idle, scan_thread_idle=%d\n",scan_thread_idle);

					cv_for_scan.Wait();
					scan_thread_idle--;

				}
				else{//found an sst to search
					break;
				}
			}
		
	//*********construct local searching key begin**********			
			//printf("version_set, con search, begin construct local\n");
			current_id=scan_right->logical_file_counter;
			scan_right->logical_file_counter++;//the next thead will search the next logical file.
			
			int scan_amount=alloc_ratios[current_id]*scan_right->scan_amount;
			//printf("version_set,, scan_amount=%d tree id=%d\n", scan_amount, current_id);
			//now we alloc amount for the sst
								
			LogicalMetaData *logical_f= scan_logical_files_set[current_id];
			
			ReadOptions &options_local =scan_right->options;

			char data_for_searching_ikey[100];//Assure the key size less than 100.
			memcpy(data_for_searching_ikey, scan_right->ikey.data_ , scan_right->ikey.size());
			Slice ikey_local=Slice(data_for_searching_ikey,  scan_right->ikey.size());//make searching_ikey local

			Saver saver_ ;	//=item->saver_
			//(1) saver_.user_key must be made local. The actal data of the user_key is a pointer.
			//(2) Searching key must be made local.
			saver_.state=kNotFound;
			saver_.ucmp=scan_right->ucmp;
			saver_.user_key= Slice(ikey_local.data_, ikey_local.size()-8);//this is impaortant, make user_key local
			
			std::string  value;
			saver_.value=&value;

			int fingerprint=scan_right->fingerprint;

			
			//printf("version_set, con search, end construct local\n");
	//*********construct local searching key end**********

		mutex_for_scan.Unlock();
//*********pre concurrent search end**********	

//**********concurrent search begin******************				
		Status s;
			//printf("version_set, 000000000000\n");

		if (saver_.ucmp->Compare(saver_.user_key, logical_f->smallest.user_key()) >= 0 &&saver_.ucmp->Compare(saver_.user_key, logical_f->largest.user_key()) <= 0) {//in the logical range
						// range search.
			//printf("version_set, 11111111111111\n");
			Iterator *scan_iter=NewTwoLevelIterator( new LogicalSSTNumIterator(vset_->icmp_, logical_f), &GetFileIterator, vset_->table_cache_, options_local);	
			//printf("version_set, 222222222222222222\n");

			scan_iter->Seek(ikey_local);
			//printf("version_set, 33333333333333333333,scan_amount=%d\n",scan_amount);

			uint64_t bytes=0;
			for(int i=0;i<scan_amount;i++){
				if(scan_iter->Valid()){
						bytes=scan_iter->key().size() + scan_iter->value().size();
						scan_iter->Next();

				}
				else{
					break;

				}
			}
			delete scan_iter;
   
		}
		else{//The key is not in the range of the logical file, so do nothing.
			//saver_.state=kNotFound;	
		}
				
//**********concurrent search end******************
////**********process the search results  begin//**********
		mutex_for_scan.Lock();
			scan_right->finished++;
			if(scan_right->finished >= scan_logical_file_total){

				scan_right->should_pop=1;
			}

		mutex_for_scan.Unlock();

////**********process the search results  end//**********

	}




}

int th_counter=0;
void Version::Forest_back(){

	mutex_for_searching_queue.Lock();
		uint32_t th_id=th_counter++;
	mutex_for_searching_queue.Unlock();

	printf("versionset.cc, Forest_back, thread created, searching_right=%p, th_id=%d\n",searching_right,th_id);
	while(true){
//*********pre concurrent search begin**********			

			 Searching_item *to_search;
			
			mutex_for_searching_queue.Lock();
			int current_id;//determined by the search id of the selected logical file

			while(1){

					int found_logical_file_to_seacrh=0;								
				//get an SST to search begin
					to_search= searching_right;
					//printf("versionset, forest_back, 1111,to_search=%p\n",to_search);
					while(to_search!=NULL && (to_search->should_pop==1 || to_search->logical_file_counter >= logical_file_total) ){//This item is not needed to be searched, so we move to the later item (the previous one).
						to_search= to_search->pre;				
					}
					//Coming to here, either the item is NULL, or it is waiting to be serviced. That is, to_search->should_pop is 0, and logical_file_counter < logical_file_total
					if(to_search==NULL){//Searching queue can be regarded as empty.
						//printf("versionset, forest_back, to_search=%p\n",to_search);
						found_logical_file_to_seacrh=0;
					}
					else if( to_search->should_pop ==0 && to_search->logical_file_counter < logical_file_total){//Found an SST to search. To perform the search operation.
					
						found_logical_file_to_seacrh=1; 						
					}
					else{
						fprintf(stderr,"version_set, Forest_back,err\n");
						exit(9);
										
					}
					
				//get an SST to search end
				
					if(0==found_logical_file_to_seacrh){//go to wait for the next request
						thread_idle++;
						//printf("versionset, forest_back, begin waiting, thread_idle=%d\n",thread_idle);
						cv_for_searching.Wait();//Unlock the mutex_for_searching_queue and wait on it.
						thread_idle--;// Lock the mutex_for_searching_queue.
						//printf("versionset, forest_back, after waiting, thread_idle=%d\n",thread_idle);
						continue;

					}
					else if(1==found_logical_file_to_seacrh){//found an sst to search
						break;
					}
					
					printf("versionset, error,111111111 exit\n");
					exit(9);
			}
		
	//*********construct local searching key begin**********			
			//printf("version_set, con search, begin construct local\n");
			
			current_id=to_search->logical_file_counter;
			to_search->logical_file_counter++;//the next thread will search the next logical file.
								
			LogicalMetaData *logical_f= logical_files_set[current_id];
			
			ReadOptions &options_local =to_search->options;

			char data_for_searching_ikey[100];//Assume that the key size is less than 100 bytes.
			memcpy(data_for_searching_ikey, to_search->ikey.data_ , to_search->ikey.size());//This is important!
			Slice ikey_local=Slice(data_for_searching_ikey,  to_search->ikey.size());//Make the searching_ikey local

			Saver saver_ ;	//=item->saver_
			//(1) saver_.user_key must be made local. The actual data of the user_key is a pointer.
			//(2) Searching key must be made local.
			saver_.state=kNotFound;
			saver_.ucmp=to_search->ucmp;
			saver_.user_key= Slice(ikey_local.data_, ikey_local.size()-8);//this is impaortant, make user_key local
			
			std::string  value;
			saver_.value=&value;

			int fingerprint=to_search->fingerprint;//This is set by the front server thread.
			//printf("version_set, con search, end construct local\n");
	//*********construct local searching key end**********

		mutex_for_searching_queue.Unlock();
//*********pre concurrent search end**********			
		
//**********concurrent search begin******************				
		Status s;
		if (saver_.ucmp->Compare(saver_.user_key, logical_f->smallest.user_key()) >= 0 &&saver_.ucmp->Compare(saver_.user_key, logical_f->largest.user_key()) <= 0) {
						// range search.
			uint32_t index = FindFile(vset_->icmp_, *logical_f, ikey_local);	
			if (index >= logical_f->physical_files.size()) {
							//do nothing. no physical file responses to this key.
				//saver_.state=kNotFound;	
			}
			else if (saver_.ucmp->Compare(saver_.user_key, logical_f->physical_files[index].smallest.user_key()) >= 0){//find a physical file	
					PhysicalMetaData *physical_f= &(logical_f->physical_files[index]);//
					if (saver_.ucmp->Compare(saver_.user_key, physical_f->smallest.user_key())< 0 ||
						saver_.ucmp->Compare(saver_.user_key, physical_f->largest.user_key()) > 0) {//Not contained in this physical file.
							//saver_.state=kNotFound;				
					}
					else{//Possibely contained in this SST file.
						uint64_t physical_file_number= physical_f->number;
						uint64_t physical_file_size= physical_f->file_size;
						s = vset_->table_cache_->Get(options_local, physical_file_number, physical_file_size, ikey_local, &saver_, SaveValue);
					}
			}      
		}
		else{//The key is not in the range of the logical file, so do nothing.
			//saver_.state=kNotFound;	
		}
				
//**********concurrent search end******************
	

////**********process the search results  begin//**********
		mutex_for_searching_queue.Lock();
		//printf("version_set.cc,check begin, th_id=%d\n",th_id);
			int should_grant_value=0;
			if(to_search==NULL){//There is possibility that the item is deleted by the front thread.
			
			}
			else if(to_search->should_pop ==1 ||fingerprint!=to_search->fingerprint || to_search->ikey!= ikey_local ){//The should_pop has been set by another thread, or the fingerprint is not the one of to_search. There is no need to check.
				//printf("version_set.cc,  con search, to_search=null\n");
				//exit(9);
			}		
			else{//***********checking and operating begin****************			
				int check_num=logical_file_total;//default to check all results.

				if(kFound == saver_.state){
					//printf("version_set.cc, kFound\n");
					to_search->search_result_of_ssts[current_id]=2;//Mark the id for this result as Found.  
					check_num=current_id;//If the key is found in this thread, all later results need not to be checked
					to_search->found_flag=1;//This is a bug, but now we use it because we do not consider deletions.
					should_grant_value=1;
				}
				else{
					to_search->search_result_of_ssts[current_id]=1;//Mark the id for this result as NotFound.
				}

				int should_pop_local=1;//Defaulting the flag to be 1.

				for(int i=0;i<check_num;i++){
					if(0==to_search->search_result_of_ssts[i]){//The logical file of this id has not been searched, so at least the thread that searches this logcial file will response the final check.	//end the for, and not pop								
						should_pop_local=0;
						break;
					}
					else if(2==to_search->search_result_of_ssts[i]){//All the front results must be one, saying that they have finished the searches, so this result of Found is the final result and we can conclude the search.				
						should_pop_local=1;
						should_grant_value=0;//There is more sound result has granted the value.
						break;
					}
				}	

				if(should_grant_value==1){
					to_search->value=saver_.value;
				}
				//Note that if the current result is one, the threads will check all the results. And if all the results are one, this thread response for the pop.
				
				if(should_pop_local==1){
					//printf("version_set.cc, get end, pop, item->found_flag=%d\n",item->found_flag);
					//cv_for_delaying.SignalAll();//we use spin lock in the main thread.
					to_search->should_pop=1;//Marks so that the main thread returns the result.
					if(to_search->found_flag==1){
						//to_search->saver_.state=kFound;
						to_search->final_state=1;
					}
				}
			}//***********checking and operat end****************
			
			//printf("version_set.cc,check end, th_id=%d, thread_idle=%d\n",th_id,thread_idle);
		mutex_for_searching_queue.Unlock();
////**********process the search results  end//**********		
	}
}

Status Version::Scan(const ReadOptions& options, const LookupKey& k, void **buf,
             GetStats* stats, int amount){

	Slice ikey = k.internal_key();
	Slice user_key = k.user_key();
	const Comparator* ucmp = vset_->icmp_.user_comparator();

   if(!scan_has_created){
		scan_thread_idle=0;
		scan_right=NULL;

		int scan_threads_num=read_thread;
		Create_scan_threads(scan_threads_num);

		scan_has_created=true;
		scan_logical_file_total=0;
		uint64_t total_size=0;
		for (int level = 0; level < config::kNumLevels; level++) {			
			printf("versionset, level=%d, logical_files_[level].size()=%d\n",level,logical_files_[level].size());
			for(int j=0;j<logical_files_[level].size();j++){

				total_size+= logical_files_[level][j]->file_size/1024/1024;
				scan_logical_files_set[scan_logical_file_total] = logical_files_[level][j];							
				scan_logical_file_total++;
			}
			
		}
		for(int i=0;i<scan_logical_file_total;i++){
			
			alloc_ratios[i]=double(scan_logical_files_set[i]->file_size/1024/1024)/total_size;

			printf("versionset, scan,i=%d,size=%ld,ratio=%f\n", i,scan_logical_files_set[i]->file_size/1024/1024,alloc_ratios[i] );
		}
		//exit(9);
		//usleep(10000);//Assuring all the threads are prepared (an ugly way).
		//printf("versionSet, scan, before while\n");
		while(scan_thread_idle<scan_threads_num);
		printf("versionset.cc, scan, thread created finished,scan_thread_idle=%d\n", scan_thread_idle);
	}

	if(amount==0){//for invoke
		return Status::OK();
	}
mutex_for_scan.Lock();
		scan_right= new Scan_item();;
		scan_right->ikey=ikey;;//Slice(right->data_for_ikey_, ikey.size());//construct Slice ikey for the link item.		
		scan_right->options=options;//provide argument for thread search		
		scan_right->ucmp=ucmp;
		scan_right->fingerprint=rand();
		//printf("version_set, amount=%d, scan_logical_file_total=%d\n", amount, scan_logical_file_total);
		scan_right->scan_amount=amount;
		//printf("right->fingerprint %d, %d\n",right->fingerprint,rand());
mutex_for_scan.Unlock();

		//printf("version_set,scan, before signalall\n");
		cv_for_scan.SignalAll();	//wake the searching threads.
	
		while(scan_right->should_pop!=1){//poll the result status;
			
		}
		mutex_for_scan.Lock();	
		delete scan_right;
		scan_right=NULL;
		mutex_for_scan.Unlock();

		return Status::OK();
}


Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
					
	//printf("Version_set, get, begin\n");				
 Slice ikey = k.internal_key();
 Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  

int method=read_method;// 0 is multi thread, 1 is single thread.
 //********************************lsm-forest begin**************************************************
//#define TH_NUM read_thead

if(0==method){//Multi-thread way.
	//has_created=1;
   if(!has_created){//Create thread pool. It is better to move this function out to be as a independent function.
		thread_idle=0;
		
		searching_left=NULL;
		searching_right=NULL;
		queue_size=0;

		printf("Version::Get, before Fores_thread_create, thread num=%d\n", read_thread);
		Fores_thread_create(read_thread);
		printf("Version::Get, after Fores_thread_create\n");

		has_created=true;
		logical_file_total=0;
				
		for (int level = 0; level < config::kNumLevels; level++) {			
			
			for(int j=0;j<logical_files_[level].size();j++){			
				logical_files_set[logical_file_total] = logical_files_[level][j];				
			
				logical_file_total++;
			}
			
			printf("Version::Get, logical_files_ in lev %d=%d\n", level, logical_files_[level].size());
		}
		
		//usleep(10000);//Assuring all the threads are prepared (an ugly way).
		while(thread_idle<read_thread);
		
		printf("versionset.cc, get, thread created finished,thread_idle=%d\n", thread_idle);
	}
		//clock_gettime(CLOCK_MONOTONIC,&search_begin);
		
		//printf("version set, begin, left=%p,right=%p\n", searching_left,searching_right);
//***************************************construct the search item begin
		 Searching_item *new_item= new Searching_item();;
		new_item->ikey=ikey;;//Slice(right->data_for_ikey_, ikey.size());//construct Slice ikey for the link item.		
		new_item->options= options;//provide argument for thread search		
		new_item->ucmp=ucmp;
		int thread_fingerprint= pthread_self();//Using thread id would be more simpler and safer.
		new_item->fingerprint= thread_fingerprint;
		//printf("right->fingerprint %d, th_id=%d\n",new_item->fingerprint, pthread_self());
mutex_for_searching_queue.Lock();
			new_item->next= searching_left;//Either searching_left is null or not.			
			
			if(searching_left!=NULL){//insert the new item
				searching_left->pre=new_item;
			}
			
			searching_left=new_item;
			
			if(searching_right==NULL){
				searching_right=new_item;
			}
			
			queue_size++;
mutex_for_searching_queue.Unlock();
		//printf("version set, 22222222, left=%p,right=%p\n", searching_left,searching_right);

//***************************************construct the search item end
		if(queue_size>10){
			printf("versionset, Get, queue_size=%d\n", queue_size);	
		}
		//printf("versionset.c, before SignalAll, thread_idle=%d, key=%s, queue_size=%d, should_pop=%d,rightKey=%s,fp=%d,popfp=%d\n",thread_idle, user_key.data(),queue_size,searching_right->should_pop,searching_right->ikey.data(),thread_fingerprint,searching_right->fingerprint);
		
		
		cv_for_searching.SignalAll();	//Wake the searching threads. This makes sure that the new item will be serviced. if(thread_idle> read_thread-2) 
	
		uint64_t counter=0; 
		while(searching_right!=NULL && (searching_right->should_pop!=1 || searching_right->fingerprint != thread_fingerprint) ){//poll the result status;  
			//cv_for_searching.SignalAll();
				//Go on polling		
				usleep(1);

								
		}
		
	
		
mutex_for_searching_queue.Lock();	//Needing lock because the searching threads may be accessing the item.
		Searching_item *temp = searching_right;//for delete
		searching_right=searching_right->pre;		
		if(searching_right==NULL){//The last item is deleted, and searching_left also point to that item.
			searching_left=NULL;
		}		
		queue_size--;
mutex_for_searching_queue.Unlock();

		
		int status=temp->final_state;
		value=temp->value;
		delete temp;
		
		//clock_gettime(CLOCK_MONOTONIC,&search_stage);
		//double notch =( (int)search_stage.tv_sec+((double)search_stage.tv_nsec)/s_to_ns ) - ( (int)search_begin.tv_sec+((double)search_begin.tv_nsec)/s_to_ns );

		//printf("version set, end, left=%p,right=%p\n", searching_left,searching_right);
	if(status==1){
		return Status::OK();
	}
	else return Status::NotFound(Slice());
}	 
 //********************************lsm-forest end**************************************************
  
  
  
  
  
  
  
  
  
if(1==method){  //Traditional way
  
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  //FileMetaData* last_file_read = NULL;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.
  //LogicalMetaData* tmp2;
  
  static uint64_t times_get=0, level_total=0;
  static uint64_t sst_num=0;
  times_get++;
  //if(times_get!=0) printf("version_set.cc, level_total =%d, times_get=%d, aver level=%f\n", level_total,times_get, (double)level_total/times_get);
   // if(times_get!=0) printf("version_set.cc, sst_num =%d, times_get=%d, aver sst=%f\n", level_total,times_get, (double)sst_num/times_get);

    
//to do range search for getting the ssts that possibly hold the key
		// we should push the physical files to the tmp for search  
//printf("version set, get, key=%s\n",user_key.data());
	std::vector<PhysicalMetaData*> tmp;//to hold the files to be search

  for (int level = 0; level < config::kNumLevels; level++) {
 
		tmp.clear();//mei 
		size_t num_logical_files = logical_files_[level].size(); 
		if (num_logical_files == 0) continue;
		// Get the list of files to search in this level
		LogicalMetaData* const* logical_files_in_level = &logical_files_[level][0];
    
		tmp.reserve(num_logical_files);//each logical file at most have one physical responsive physical file
		//may need to make sure the logical files in the level are sorted by time.
		for (uint32_t i = 0; i < num_logical_files; i++) {//get the physical files may be contain the keys.
			LogicalMetaData* logical_f = logical_files_in_level[i];
			if (ucmp->Compare(user_key, logical_f->smallest.user_key()) >= 0 &&ucmp->Compare(user_key, logical_f->largest.user_key()) <= 0) {
				//within logical range.
				uint32_t index = FindFile(vset_->icmp_, *logical_f, ikey);
				if (index >= logical_f->physical_files.size()) {
					//do nothing. no physical file responses to this key.
				}
				else if (ucmp->Compare(user_key, logical_f->physical_files[index].smallest.user_key()) >= 0){//find a physical file	
					tmp.push_back(&(logical_f->physical_files[index]));//tmp contains pointers
				}      
			}
		}
	
		if (tmp.empty()) continue;//continue to search the next level
      
		PhysicalMetaData* const* physical_files_to_search_old = &tmp[0]; //points to the vector of files.
		int num_physical_files = tmp.size();
  
	//to here we get the files to search for the current level.

//*************************here is old method with single traverse******************

		{	
				for (uint32_t i = 0; i < num_physical_files; ++i) { // begin to read the files by order. However, in lsm-forest here will be changed to concurrently.
				
				  PhysicalMetaData* f = physical_files_to_search_old[i];
				 
				  //printf("version set, get, lev=%d, file size=%ju MB\n", level, f->file_size/1024/1024);    
				  Saver saver;
				  saver.state = kNotFound;
				  saver.ucmp = ucmp;
				  saver.user_key = user_key;
				  saver.value = value;

				  s = vset_->table_cache_->Get(options, f->number, f->file_size, ikey, &saver, SaveValue);	
											   
				  if (!s.ok()) {
					return s;
				  }
				  switch (saver.state) {
					case kNotFound:
						//printf("version set, get, not found, lev=%d,file=%d\n",level,f->number);
					  break;      // Keep searching in other files
					case kFound:
						//printf("version set, get, found, lev=%d, file=%d--------------------\n",level,f->number);
					  return s;
					case kDeleted:
						printf("version set, get, delete, lev=%d--------------------\n",level);
						exit(9);
					  s = Status::NotFound(Slice());  // Use empty error message for speed
					  return s;
					case kCorrupt:
						printf("version set, get, corrupt, lev=%d--------------------\n",level);
						exit(9);
					  s = Status::Corruption("corrupted key for ", user_key);
					  return s;
				  }
				}
				//to here to old method ends			
		}
//*************************here is old method with single traverse end******************

	
  }//here end searching a level, may be have found and returned

}//end if method==1  
  //here to end all the searching and no key found
		//printf("version set, get, not found, key=%s\n",user_key.data());
  return Status::NotFound(Slice());  // Use an empty error message for speed
}



bool Version::UpdateStats(const GetStats& stats) {
  //FileMetaData* f = stats.seek_file;
	LogicalMetaData* f = stats.seek_file;
  if (f != NULL) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == NULL) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, LogicalMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  //ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

// bool Version::OverlapInLevel(int level,
                             // const Slice* smallest_user_key,
                             // const Slice* largest_user_key) {
  // return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               // smallest_user_key, largest_user_key);
// }

// int Version::PickLevelForMemTableOutput(
    // const Slice& smallest_user_key,
    // const Slice& largest_user_key) {
  // int level = 0;
  // if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // // Push to next level if there is no overlap in next level,
    // // and the #bytes overlapping in the level after that are limited.
    // InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    // InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    // std::vector<FileMetaData*> overlaps;
    // while (level < config::kMaxMemCompactLevel) {
      // if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // break;
      // }
      // if (level + 2 < config::kNumLevels) {
        // // Check that file does not overlap too many grandparent bytes.
        // GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        // const int64_t sum = TotalFileSize(overlaps);
        // if (sum > kMaxGrandParentOverlapBytes) {
          // break;
        // }
      // }
      // level++;
    // }
  // }
  // return level;
// }

// Store in "*inputs" all files in "level" that overlap with [begin,end]
// void Version::GetOverlappingInputs(
    // int level,
    // const InternalKey* begin,
    // const InternalKey* end,
    // std::vector<FileMetaData*>* inputs) {
  // assert(level >= 0);
  // assert(level < config::kNumLevels);
  // inputs->clear();
  // Slice user_begin, user_end;
  // if (begin != NULL) {
    // user_begin = begin->user_key();
  // }
  // if (end != NULL) {
    // user_end = end->user_key();
  // }
  // const Comparator* user_cmp = vset_->icmp_.user_comparator();
  // for (size_t i = 0; i < files_[level].size(); ) {
    // FileMetaData* f = files_[level][i++];
    // const Slice file_start = f->smallest.user_key();
    // const Slice file_limit = f->largest.user_key();
    // if (begin != NULL && user_cmp->Compare(file_limit, user_begin) < 0) {
      // // "f" is completely before specified range; skip it
    // } else if (end != NULL && user_cmp->Compare(file_start, user_end) > 0) {
      // // "f" is completely after specified range; skip it
    // } else {
      // inputs->push_back(f);
      // if (level == 0) {
        // // Level-0 files may overlap each other.  So check if the newly
        // // added file has expanded the range.  If so, restart search.
        // if (begin != NULL && user_cmp->Compare(file_start, user_begin) < 0) {
          // user_begin = file_start;
          // inputs->clear();
          // i = 0;
        // } else if (end != NULL && user_cmp->Compare(file_limit, user_end) > 0) {
          // user_end = file_limit;
          // inputs->clear();
          // i = 0;
        // }
      // }
    // }
  // }
// }

std::string Version::DebugString() const {
  std::string r;
  // for (int level = 0; level < config::kNumLevels; level++) {
    // // E.g.,
    // //   --- level 1 ---
    // //   17:123['a' .. 'd']
    // //   20:43['e' .. 'g']
    // r.append("--- level ");
    // AppendNumberTo(&r, level);
    // r.append(" ---\n");
    // const std::vector<FileMetaData*>& files = files_[level];
    // for (size_t i = 0; i < files.size(); i++) {
      // r.push_back(' ');
      // AppendNumberTo(&r, files[i]->number);
      // r.push_back(':');
      // AppendNumberTo(&r, files[i]->file_size);
      // r.append("[");
      // r.append(files[i]->smallest.DebugString());
      // r.append(" .. ");
      // r.append(files[i]->largest.DebugString());
      // r.append("]\n");
    // }
  // }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(LogicalMetaData* f1, LogicalMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<LogicalMetaData*, BySmallestKey> FileSet;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base)
      : vset_(vset),
        base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<LogicalMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin();
          it != added->end(); ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        LogicalMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {//edit contains added logical files and deleted logical files
    // Update compaction pointers
    // for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {//we may not need this 
      // const int level = edit->compact_pointers_[i].first;
      // vset_->compact_pointer_[level] =
          // edit->compact_pointers_[i].second.Encode().ToString();
    // }

    // Delete files
	//printf("version_set,builder,Apply, begin\n");
    const VersionEdit::DeletedLogicalFileSet& del = edit->deleted_logical_files_;
    for (VersionEdit::DeletedLogicalFileSet::const_iterator iter = del.begin();iter != del.end(); ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
	  //printf("version_set, apply, delete number=%d\n",number);
    }
	//printf("version_set,builder,Apply, 111111111111111\n");

    // Add new files
	for(int i=0;i< edit->new_logical_files_.size(); i++){
	
	
		const int level = edit->new_logical_files_[i].first;
		
		LogicalMetaData* f = new LogicalMetaData(edit->new_logical_files_[i].second); //why use new? The leveldb guys are realy clever
		f->refs = 1;
		f->allowed_seeks = (f->file_size / 16384);
		if (f->allowed_seeks < 100) f->allowed_seeks = 100;

		levels_[level].deleted_files.erase(f->number);//erase to not delete the file

		levels_[level].added_files->insert(f);

	}	
	// if(edit->new_logical_file.physical_files.size()>0){
		// const int level = edit->level_of_new_logical_file;
		// LogicalMetaData* f = new LogicalMetaData(edit->new_logical_file); //why use new? The leveldb guys are realy clever
		// f->refs = 1;
		// f->allowed_seeks = (f->file_size / 16384);
		// if (f->allowed_seeks < 100) f->allowed_seeks = 100;

		// levels_[level].deleted_files.erase(f->number);//erase to not delete the file

		// levels_[level].added_files->insert(f);

	// }
    // for (size_t i = 0; i < edit->new_files_.size(); i++) {
      // const int level = edit->new_files_[i].first;
      // FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      // f->refs = 1;

      // // We arrange to automatically compact this file after
      // // a certain number of seeks.  Let's assume:
      // //   (1) One seek costs 10ms
      // //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      // //   (3) A compaction of 1MB does 25MB of IO:
      // //         1MB read from this level
      // //         10-12MB read from next level (boundaries may be misaligned)
      // //         10-12MB written to next level
      // // This implies that 25 seeks cost the same as the compaction
      // // of 1MB of data.  I.e., one seek costs approximately the
      // // same as the compaction of 40KB of data.  We are a little
      // // conservative and allow approximately one seek for every 16KB
      // // of data before triggering a compaction.
      // f->allowed_seeks = (f->file_size / 16384);
      // if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      // levels_[level].deleted_files.erase(f->number);
      // levels_[level].added_files->insert(f);
    // }
  }

  // Save the current state in *v.
  void SaveTo(Version* v) {
	//printf("version_set,builder,SaveTo, begin\n");
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {//I think here is only one file of one level, so we can optimize it.
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<LogicalMetaData*>& base_files = base_->logical_files_[level];//Version* base_;
      std::vector<LogicalMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<LogicalMetaData*>::const_iterator base_end = base_files.end();
	  
      const FileSet* added = levels_[level].added_files;//FileSet* added_files; typedef std::set<LogicalMetaData*, BySmallestKey> FileSet;
      v->logical_files_[level].reserve(base_files.size() + added->size());
	  //add the base files
	  for (; base_iter != base_end; ++base_iter) {
         MaybeAddFile(v, level, *base_iter);
      }
	  
	  //add the new file
	  //assure that the size of added is 1.
	for (FileSet::const_iterator added_iter = added->begin(); added_iter != added->end();++added_iter) {
			//FileSet::const_iterator added_iter = added->begin();
			MaybeAddFile(v, level, *added_iter);
	}
	  
      // for (FileSet::const_iterator added_iter = added->begin();added_iter != added->end();++added_iter) {//add all the added files
        // // Add all smaller files listed in base_
        // for (std::vector<LogicalMetaData*>::const_iterator bpos = std::upper_bound(base_iter, base_end, *added_iter, cmp); base_iter != bpos; ++base_iter) {
          // MaybeAddFile(v, level, *base_iter);
        // }//we directly add, because logical files are not needed in order.

        // MaybeAddFile(v, level, *added_iter);
      // }

      // // Add remaining base files
       // for (; base_iter != base_end; ++base_iter) {
         // MaybeAddFile(v, level, *base_iter);
       // }
	   
	   

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      // if (level > 0) {
		// printf("saveto, leve >0, check overlap\n");
        // for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          // const InternalKey& prev_end = v->files_[level][i-1]->largest;
          // const InternalKey& this_begin = v->files_[level][i]->smallest;
          // if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            // fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                    // prev_end.DebugString().c_str(),
                    // this_begin.DebugString().c_str());
            // abort();
          // }
        // }
      // }
#endif
    }

		//printf("version_set,builder,SaveTo, end\n");
  }

  void MaybeAddFile(Version* v, int level, LogicalMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<LogicalMetaData*>* files = &v->logical_files_[level];
      // if (level > 0 && !files->empty()) {
        // // Must not overlap
        // assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest,
                                    // f->smallest) < 0);
      // }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      dummy_versions_(this),
      current_(NULL) {
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}




void VersionSet::AddFileLevelBloomFilterInfo(uint64_t file_number, std::string* filter_string) {


		printf("versionset, AddFileLevelBloomFilterInfo, file=%d,  filter_size=%d bytes\n",file_number,  filter_string->size());
		//fwrite(, filter_string->size(), 1, filter_file);
		void *buffer=malloc(filter_string->size());
		 //filter_string->copy(buffer,6,5);
	file_level_bloom_filter[file_number] = filter_string;
	

}




int filter_counter=0;
void VersionSet::PopulateBloomFilterForFile(PhysicalMetaData* file, FileLevelFilterBuilder* file_level_filter_builder) {

	printf("version set, PopulateBloomFilterForFile begin\n ");

	uint64_t file_number = file->number;
	uint64_t file_size = file->file_size;
	int cnt = 0;

	if (file_level_bloom_filter[file_number] != NULL) {
		// This means that we have already calculated the bloom filter for this file and files are immutable (wrt a file number)
		return;
	}

    Iterator* iter = table_cache_->NewIterator(ReadOptions(), file_number, file_size);
    iter->SeekToFirst();
    int index = 0;
    while (iter->Valid()) {
    	cnt++;
    	file_level_filter_builder->AddKey(iter->key());
    	index++;
    	iter->Next();
    }
    if (cnt > 0) {
		std::string* filter_string = file_level_filter_builder->GenerateFilter();
		assert (filter_string != NULL);
		
		printf("versionset, PopulateBloomFilterForFile, str length=%d\n", filter_string->size());
		//fprintf(filter_file,"%d %d\n",file_number,filter_string->size());
		for(int i=0;i<filter_string->size();i++){
			fprintf(filter_file,"%c",(*filter_string)[i]);
		}
		fprintf(filter_file,"\n");
		fflush(filter_file);
		
		filter_counter++;
		
		if(filter_counter>=2){
			exit(9);
		}
		AddFileLevelBloomFilterInfo(file_number, filter_string);
    }
    delete iter;
}




void VersionSet::Generate_file_level_bloom_filter(){
	
	printf("version set, Generate_file_level_bloom_filter begin\n ");
	//If the file named "file_level_bloom_filter" does not exist, generate the file.
	//Else, read the file to generate filter.
	
	 filter_file=fopen("file_level_bloom_filter","w+");
	
	 const FilterPolicy *filter_policy = options_->filter_policy;
	FileLevelFilterBuilder file_level_filter_builder(filter_policy);
	
    if (filter_policy == NULL) {
		printf("version set, Generate_file_level_bloom_filter, filter_policy is NULL, do not Generate_file_level_bloom_filter\n");
    	return;
    }


    Version* current = current_;
	current->Ref();
	for (int i = 0; i < config::kNumLevels; i++) {
		for(int j=0;j< current->logical_files_[i].size();j++){
			for(int k=0;k<current->logical_files_[i][j]->physical_files.size(); k++){
				PopulateBloomFilterForFile( &(current->logical_files_[i][j]->physical_files[k]), &file_level_filter_builder);
			}
		}
	}
	//file_level_filter_builder.Destroy();
	current->Unref();	

	exit(9);

}














void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  //************lsm-forest
    //current_->env_forest = env_;
	//current_->id_thread=0;
  //************lsm-forest end************

  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {

		//printf("version_set, LogAndApply, begin\n");

		//printf("version_set, LogAndApply, logical id=%d, phy size=%d \n", edit->new_logical_file.number,edit->new_logical_file.physical_files.size());
		
			//for(int i=0;i<current_->files_[0].size();i++){
				// printf("%d ",current_->files_[0][i]->number);
			// }
			// printf("\n");
		
		  if (edit->has_log_number_) {
			assert(edit->log_number_ >= log_number_);
			assert(edit->log_number_ < next_file_number_);
		  } else {
			edit->SetLogNumber(log_number_);
		  }

		  if (!edit->has_prev_log_number_) {
			edit->SetPrevLogNumber(prev_log_number_);
		  }

		  edit->SetNextFile(next_file_number_);
		  edit->SetLastSequence(last_sequence_);

		  Version* v = new Version(this);
		  {
			Builder builder(this, current_);
			builder.Apply(edit);
			builder.SaveTo(v);
			
			//printf("version_set, LogAndApply, current_ 0 size=%d, v 0 size=%d\n", current_->files_[0].size(),v->files_[0].size());
		  }
		   
		  //Finalize(v);
		  
		  // Initialize new descriptor log file if necessary by creating
		  // a temporary file that contains a snapshot of the current version.
		  std::string new_manifest_file;
		  Status s;
		  if (descriptor_log_ == NULL) {
			// No reason to unlock *mu here since we only hit this path in the
			// first call to LogAndApply (when opening the database).
			assert(descriptor_file_ == NULL);
			new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
			edit->SetNextFile(next_file_number_);
			s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
			if (s.ok()) {
			  descriptor_log_ = new log::Writer(descriptor_file_);
			  s = WriteSnapshot(descriptor_log_);
			}
		  }

		  // Unlock during expensive MANIFEST log write
		  {
			//mu->Unlock();

			// Write new record to MANIFEST log
			if (s.ok()) {
			  std::string record;
			  edit->EncodeTo(&record);
			  s = descriptor_log_->AddRecord(record);
			  if (s.ok()) {
				s = descriptor_file_->Sync();
			  }
			  if (!s.ok()) {
				Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
			  }
			}

			// If we just created a new descriptor file, install it by writing a
			// new CURRENT file that points to it.
			if (s.ok() && !new_manifest_file.empty()) {
			  s = SetCurrentFile(env_, dbname_, manifest_file_number_);
			}

			//mu->Lock();
		  }

		  // Install the new version
		  if (s.ok()) {
			AppendVersion(v);
			//printf("version_set, LogAndApply,after AppendVersion,current_ 0 size=%d, v 0 size=%d\n", current_->files_[0].size(),v->files_[0].size());
			  //printf("version_set, LogAndApply, after AppendVersion, lev 0 need comp=%d,\n", Need_compact(0));
			log_number_ = edit->log_number_;
			prev_log_number_ = edit->prev_log_number_;
		  } else {
			delete v;
			if (!new_manifest_file.empty()) {
			  delete descriptor_log_;
			  delete descriptor_file_;
			  descriptor_log_ = NULL;
			  descriptor_file_ = NULL;
			  env_->DeleteFile(new_manifest_file);
			}
		  }

		// printf("LogAndApply, end\n");
		// for(int i=0;i<current_->files_[0].size();i++){
				// printf("%d ",current_->files_[0][i]->number);
		// }
		// printf("\n");
		  return s;
}

int VersionSet::Test(){
	for(int i=0;i<10;i++){
		printf("version_set.cc, Lev %d, logical num %d\n",i, current_->logical_files_[i].size());
	
	}
}
Status VersionSet::Recover() {
	printf("version_set, begin\n");
	//Test();
	//exit(0);
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };
	
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  //printf("version_set, 0000000, s.ok()=%d\n", s.ok());
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
 //printf("version_set, 22222222, s.ok()=%d\n", s.ok());
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
	printf("version_set, 3333333333, s.ok()=%d\n", s.ok());
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }
 printf("version_set, 44444444444, s.ok()=%d\n", s.ok());

      if (s.ok()) {
        builder.Apply(&edit);
      }
 //printf("version_set, 5555555555555, s.ok()=%d\n", s.ok());

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = NULL;
 //printf("version_set, 888888888888, s.ok()=%d\n", s.ok());
  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }
 //printf("version_set, 99999999999, s.ok()=%d\n", s.ok());

  if (s.ok()) {
	 //printf("version_set, aaaaaaaaaa, s.ok()=%d\n", s.ok());

    Version* v = new Version(this);
	 //printf("version_set, bbbbbbbbb, s.ok()=%d\n", s.ok());

    builder.SaveTo(v);
    // Install recovered version
	 //printf("version_set, ddddddddddd, s.ok()=%d\n", s.ok());

    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
  }
		printf("version_set, end\n");
		//Test();
		//exit(9);
  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  // int best_level = -1;
  // double best_score = -1;
  // int fblevel=-1;
  // int fbscore=-1;


  // for (int level = 0; level < config::kNumLevels-1; level++) {
    // double score;
	 // int fscore=-1;
    // if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      //score = v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);//
		// score = v->logical_files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);
    // } else {
      // // Compute the ratio of current size to size limit.
      // const uint64_t level_bytes = TotalFileSize(v->logical_files_[level]);
      // score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
    // }

	//printf("versionset, Finalize, L%d, size=%d\n",level,v->files_[level].size());
	
	// if(v->logical_files_[level].size()>COMP_THRESH){//if size exceeds this value, incurr compaction.
		// fscore=v->logical_files_[level].size()-COMP_THRESH;
	
	// }
	// if(fscore>fbscore){
		// fblevel=level;
		// fbscore=fscore;
	// }
    // if (score > best_score) {
      // best_level = level;
      // best_score = score;
    // }
  //}
  //printf("versionset, finalize,fblevel=%d,fbscore=%d, v->files_[0].size()=%d, v->compaction_score_%f\n",fblevel,fbscore,v->files_[0].size(), v->compaction_score_);

  //exit(9);
 // v->compaction_level_ = fblevel;
  //v->compaction_score_ = fbscore ;//-1;// fbscore;
    fprintf(stderr,"versionset, finalize, blank, do nothing\n");
	//exit(9);
  //v->compaction_level_ = best_level;
  //v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?
	printf("version_set.cc, WriteSnapshot, begin\n");
	//exit(9);
  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<LogicalMetaData*>& files = current_->logical_files_[level];
	//printf("version_set, WriteSnapshot, lev %d, logical num %d\n",level, files.size());
    for (size_t i = 0; i < files.size(); i++) {
      const LogicalMetaData* f = files[i];
      edit.AddLogicalFile(level, *f);
    }
  }
		//printf("version_set, exit\n");
		//exit(9);
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->logical_files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  //assert(config::kNumLevels == 7);

	fprintf(stderr, "version_set.cc LevelSummary, exit\n");
	exit(9);
  // snprintf(scratch->buffer, sizeof(scratch->buffer),
           // "files[ %d %d %d %d %d %d %d ]",
           // int(current_->files_[0].size()),
           // int(current_->files_[1].size()),
           // int(current_->files_[2].size()),
           // int(current_->files_[3].size()),
           // int(current_->files_[4].size()),
           // int(current_->files_[5].size()),
           // int(current_->files_[6].size()));
  // return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<LogicalMetaData*>& files = v->logical_files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        //if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          //break;
        //}
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != NULL) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
	
  for (Version* v = dummy_versions_.next_;v != &dummy_versions_;v = v->next_) {
		
	//printf("version_set.cc, v=%p, current_=%p\n",v, current_);
    for (int level = 0; level < config::kNumLevels; level++) {
      //const std::vector<FileMetaData*>& files = v->files_[level];
		const std::vector<LogicalMetaData*>& logical_files = v->logical_files_[level];
			//printf("version_set.cc, AddLiveFiles, lev=%d, Num_logical_file=%d \n",level,v->logical_files_[level].size());
		for(size_t i=0;i<logical_files.size();i++){
			for(size_t j=0;j<logical_files[i]->physical_files.size();j++){
				live->insert(logical_files[i]->physical_files[j].number);
			}
		}
		//for (size_t i = 0; i < files.size(); i++) {
			//live->insert(files[i]->number);
		//}
	
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->logical_files_[level]);
}

int64_t VersionSet::MaxNextLevelOverlappingBytes() {

	printf("version_set.cc, MaxNextLevelOverlappingBytes,exit\n");
	exit(9);
  // int64_t result = 0;
  // std::vector<FileMetaData*> overlaps;
  // for (int level = 1; level < config::kNumLevels - 1; level++) {
    // for (size_t i = 0; i < current_->files_[level].size(); i++) {
      // const FileMetaData* f = current_->files_[level][i];
      // current_->GetOverlappingInputs(level+1, &f->smallest, &f->largest,
                                     // &overlaps);
      // const int64_t sum = TotalFileSize(overlaps);
      // if (sum > result) {
        // result = sum;
      // }
    // }
  // }
  // return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange(const std::vector<LogicalMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
						  
	printf("version_set.cc, GetRange,exit\n");
	exit(9);
  // assert(!inputs.empty());
  // smallest->Clear();
  // largest->Clear();
  // for (size_t i = 0; i < inputs.size(); i++) {
    // FileMetaData* f = inputs[i];
    // if (i == 0) {
      // *smallest = f->smallest;
      // *largest = f->largest;
    // } else {
      // if (icmp_.Compare(f->smallest, *smallest) < 0) {
        // *smallest = f->smallest;
      // }
      // if (icmp_.Compare(f->largest, *largest) > 0) {
        // *largest = f->largest;
      // }
    // }
  // }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
void VersionSet::GetRange2(const std::vector<LogicalMetaData*>& inputs1,
                           const std::vector<LogicalMetaData*>& inputs2,
                           InternalKey* smallest,
                           InternalKey* largest) {
  std::vector<LogicalMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}


Iterator* VersionSet::MakeInputIterator_conca(Compaction* c) {

	//fprintf(stderr,"version_set.cc, MakeInputIterator,exit\n");
	//exit(9);
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  //const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  //const int space =  c->inputs_[0].size();//number of iteraters
	const int space =  c->logical_files_inputs_.size();//number of iteraters
   Iterator** list = new Iterator*[space];
   int num = 0;
//printf("version_set.cc, MakeInputIterator,33333333333333333\n");
   
   for(int i=0;i<space;i++){
		// std::vector<PhysicalMetaData*> SSTlist;
		// SSTlist.clear();
		// for(int j=0; j< c->logical_files_inputs_[i]->physical_files.size();j++){
			// SSTlist.push_back( &(c->logical_files_inputs_[i]->physical_files[j]) );
		// }
        list[num++] = NewTwoLevelIterator(
            new Version::LogicalSSTNumIterator(icmp_, c->logical_files_inputs_[i]),
            &GetFileIterator, table_cache_, options);		
   
   }
//printf("version_set.cc, MakeInputIterator,55555555555555\n");

  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
//printf("version_set.cc, MakeInputIterator,end\n");

  return result;
}
Iterator* VersionSet::MakeInputIterator_from_group(std::vector<PhysicalMetaData*>& group){
	// printf("version set.cc, meta size=%d\n", sizeof(PhysicalMetaData));
		// exit(9);

	ReadOptions options;
	options.verify_checksums = options_->paranoid_checks;
	options.fill_cache = false;

	const int space =  group.size();
	Iterator** list = new Iterator*[space];
	int num = 0;
	//const std::vector<PhysicalMetaData*>& files = group;
	for (size_t i = 0; i < group.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, group[i]->number, group[i]->file_size);
	}
	 Iterator* result = NewMergingIterator(&icmp_, list, num);
	delete[] list;
	return result;
}
Iterator* VersionSet::MakeInputIterator(Compaction* c) {

	fprintf(stderr,"version_set.cc, MakeInputIterator,exit\n");
	exit(9);
  // ReadOptions options;
  // options.verify_checksums = options_->paranoid_checks;
  // options.fill_cache = false;

  // // Level-0 files have to be merged together.  For other levels,
  // // we will make a concatenating iterator per level.
  // // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // //const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  // //const int space =  c->inputs_[0].size();//number of iteraters
	// const int space =  c->logical_files_inputs_.size();//number of iteraters
  // Iterator** list = new Iterator*[space];
  // int num = 0;
  // for (int which = 0; which < 2; which++) {//inputs[0] and inputs[1], however, lsm-forest only has inputs[0]
	// if(!c->inputs_[1].empty() ){
		// printf("versionset.cc, MakeInputIterator, inputs[1] is not empty\n");
		// exit(1);
	// }
    // if (!c->inputs_[which].empty()) {//inputs[1] must be empty
		// const std::vector<FileMetaData*>& files = c->inputs_[which];
        // for (size_t i = 0; i < files.size(); i++) {
          // list[num++] = table_cache_->NewIterator(
              // options, files[i]->number, files[i]->file_size);
		// }
      // if (c->level() + which == 0) {//level is 0 and inputs is 0. For LSM-tree each level is like this
        // const std::vector<FileMetaData*>& files = c->inputs_[which];
        // for (size_t i = 0; i < files.size(); i++) {
          // list[num++] = table_cache_->NewIterator(
              // options, files[i]->number, files[i]->file_size);
        // }
      // } else {
        // // Create concatenating iterator for the files from this level
        // list[num++] = NewTwoLevelIterator(
            // new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            // &GetFileIterator, table_cache_, options);
      // }
    // }
  // }
  // assert(num <= space);
  // Iterator* result = NewMergingIterator(&icmp_, list, num);
  // delete[] list;
  // return result;
}

//COMP_THRESH
int VersionSet::Need_compact(int level) {
		if(current_->logical_files_[level].size() < growth_factor){//the level need no compaction
			return 0;
		}
		else if(current_->logical_files_[level+1].size() >= growth_factor*1.5){//the next level has no space
			return 0;
		}
		else {
			return 1;
		}
}
Compaction* VersionSet::PickLogicalFiles(int level) {
		
	Compaction* c;		
	const bool size_compaction = Need_compact(level);
	   
	if (size_compaction) {			
			assert(level >= 0);//mei
			assert(level+1 < config::kNumLevels);
			c = new Compaction(level);

			//我们可能想先选择较老的10个文件
			//int roof=current_->files_[level].size()-1;
			//int floor=current_->files_[level].size()-COMP_THRESH;
			 //printf("versionset,pickcomp, 66666666666, roof=%d, floor=%d\n",roof,floor);
			//for (int i = roof; i >=floor; i--) {//current_->files_[level].size() //pick to compact
			for(int i=0;i<growth_factor;i++){
				assert( current_->files_[level].size()>=growth_factor );			
				 //printf("versionset,pickcomp, 77777777777\n");
				LogicalMetaData* f = current_->logical_files_[level][i];
				//printf("versionset,pickcomp, i=%d, file=%d\n",i,f->number);
				c->logical_files_inputs_.push_back(f);//mei, push multi files 
			  
			}
			if (c->logical_files_inputs_.empty()) {//exception
			  // Wrap-around to the beginning of the key space
			  printf("versionset, pick, wrap back\n");
			  exit(1);
			  //c->logical_files_inputs_[0].push_back(current_->files_[level][0]);
			}
	} 
	else{
		return NULL;
	}
	
	c->input_version_ = current_;
	c->input_version_->Ref();

		//printf("pick end, c inputs 0 size:%d\n",c->inputs_[0].size() );
	//InternalKey smallest, largest;//this is for set compaction pointer in old way
	//GetRange(c->inputs_[0], &smallest, &largest);
	return c;
}

Compaction* VersionSet::PickCompaction() {//blank

	fprintf(stderr, "version_set.cc, PickCompaction,exit\n");
	exit(9);
  // Compaction* c;
  // int level;

	// //return NULL;
  // // We prefer compactions triggered by too much data in a level over
  // // the compactions triggered by seeks.
  // //printf("versionset,pickcomp,comp level=%d\n",current_->compaction_level_);
  // const bool size_compaction = (current_->compaction_score_ >= 1);
   // //printf("size_compaction=%d, current_->compaction_score_=%f\n",size_compaction,current_->compaction_score_);
   // //exit(9);
  // //return NULL;
  // const bool seek_compaction = NULL;// (current_->file_to_compact_ != NULL);
 
  
  // if (size_compaction) {
    // level = current_->compaction_level_;//我们已经得到了层。
    // assert(level >= 0);//mei
    // assert(level+1 < config::kNumLevels);
    // c = new Compaction(level);

    // // Pick the first file that comes after compact_pointer_[level]
			// //我们选择的是指针后的10个文件。
    // for (size_t i = 0; i <COMP_THRESH ; i++) {//current_->files_[level].size() //pick to compact
		// //printf("versionset, pick, L %d, i=%d, curfile=%d, totalfile=%d\n",level,i, current_->files_[level][i]->number, current_->files_[level].size());
		// //exit(9);
			// printf("PickCompaction,i=%d,file number=%d\n", i,  current_->files_[level][i]->number);
		 // FileMetaData* f = current_->files_[level][i];
		 // c->inputs_[0].push_back(f);//mei, push multi files 
      // // if (compact_pointer_[level].empty() ||
          // // icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {//select file according to compact_pointer
		  // // printf("versionset, pick, break for\n");
        // // c->inputs_[0].push_back(f);
        // // break;
      // // }
    // }
    // if (c->inputs_[0].empty()) {
      // // Wrap-around to the beginning of the key space
	  // printf("versionset, pick, wrap back\n");
	  // exit(1);
      // c->inputs_[0].push_back(current_->files_[level][0]);
    // }
  // } else if (seek_compaction) {//may should be removed, mei doesn't want compaction in seek
		// printf("versionset, pick, seek_compaction\n");
		 // exit(9);
		// level = current_->file_to_compact_level_;
		// c = new Compaction(level);
		// c->inputs_[0].push_back(current_->file_to_compact_);
  // } else {
		// return NULL;
  // }

  // c->input_version_ = current_;
  // c->input_version_->Ref();

  // // Files in level 0 may overlap each other, so pick up all overlapping ones
  

  // //SetupOtherInputs(c); //mei, we don't set up others. We have assured the participating sst.
	// InternalKey smallest, largest;
	// GetRange(c->inputs_[0], &smallest, &largest);
	// //printf("versionset. pick, smallest.Encode().ToString()=%s  largest.Encode().ToString()=%s\n", smallest.Encode().ToString().c_str(), largest.Encode().ToString().c_str());
  // //compact_pointer_[level] = largest.Encode().ToString();//mei
  // //c->edit_.SetCompactPointer(level, largest);//mei
	// //printf("versionset. pick, c->inputs_[0] size=%d\n",c->inputs_[0].size());
	// return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {//blank

	fprintf(stderr,"version_set.cc, SetupOtherInputs,exit\n");
	exit(9);
	
  // const int level = c->level();
  // InternalKey smallest, largest;
  // GetRange(c->inputs_[0], &smallest, &largest);

  // current_->GetOverlappingInputs(level+1, &smallest, &largest, &c->inputs_[1]);

  // // Get entire range covered by compaction
  // InternalKey all_start, all_limit;
  // GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // // See if we can grow the number of inputs in "level" without
  // // changing the number of "level+1" files we pick up.
  // if (!c->inputs_[1].empty()) {
    // std::vector<FileMetaData*> expanded0;
    // current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    // const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    // const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    // const int64_t expanded0_size = TotalFileSize(expanded0);
    // if (expanded0.size() > c->inputs_[0].size() &&
        // inputs1_size + expanded0_size < kExpandedCompactionByteSizeLimit) {
      // InternalKey new_start, new_limit;
      // GetRange(expanded0, &new_start, &new_limit);
      // std::vector<FileMetaData*> expanded1;
      // current_->GetOverlappingInputs(level+1, &new_start, &new_limit,
                                     // &expanded1);
      // if (expanded1.size() == c->inputs_[1].size()) {
        // Log(options_->info_log,
            // "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            // level,
            // int(c->inputs_[0].size()),
            // int(c->inputs_[1].size()),
            // long(inputs0_size), long(inputs1_size),
            // int(expanded0.size()),
            // int(expanded1.size()),
            // long(expanded0_size), long(inputs1_size));
        // smallest = new_start;
        // largest = new_limit;
        // c->inputs_[0] = expanded0;
        // c->inputs_[1] = expanded1;
        // GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      // }
    // }
  // }

  // // Compute the set of grandparent files that overlap this compaction
  // // (parent == level+1; grandparent == level+2)
  // if (level + 2 < config::kNumLevels) {
    // current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   // &c->grandparents_);
  // }

  // if (false) {
    // Log(options_->info_log, "Compacting %d '%s' .. '%s'",
        // level,
        // smallest.DebugString().c_str(),
        // largest.DebugString().c_str());
  // }

  // // Update the place where we will do the next compaction for this level.
  // // We update this immediately instead of waiting for the VersionEdit
  // // to be applied so that if the compaction fails, we will try a different
  // // key range next time.
  // compact_pointer_[level] = largest.Encode().ToString();
  // c->edit_.SetCompactPointer(level, largest);
}

Compaction* VersionSet::CompactRange(
    int level,
    const InternalKey* begin,
    const InternalKey* end) {//blank
	
	printf("version_set.cc, CompactRange,exit\n");
	exit(9);
  // std::vector<FileMetaData*> inputs;
  // current_->GetOverlappingInputs(level, begin, end, &inputs);
  // if (inputs.empty()) {
    // return NULL;
  // }

  // // Avoid compacting too much in one shot in case the range is large.
  // // But we cannot do this for level-0 since level-0 files can overlap
  // // and we must not pick one file and drop another older file if the
  // // two files overlap.
  // if (level > 0) {
    // const uint64_t limit = MaxFileSizeForLevel(level);
    // uint64_t total = 0;
    // for (size_t i = 0; i < inputs.size(); i++) {
      // uint64_t s = inputs[i]->file_size;
      // total += s;
      // if (total >= limit) {
        // inputs.resize(i + 1);
        // break;
      // }
    // }
  // }

  // Compaction* c = new Compaction(level);
  // c->input_version_ = current_;
  // c->input_version_->Ref();
  // c->inputs_[0] = inputs;
  // SetupOtherInputs(c);
  // return c;
}

Compaction::Compaction(int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(level)),
      input_version_(NULL),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {//we have no need for this function

	printf("version_set.cc IsTrivialMove,exit\n");
	exit(9);
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // return (num_input_files(0) == 1 &&
          // num_input_files(1) == 0 &&
          // TotalFileSize(grandparents_) <= kMaxGrandParentOverlapBytes);
}

void Compaction::AddInputDeletions(VersionEdit* edit) {

	for(int i=0;i<logical_files_inputs_.size();i++){
		edit->DeleteLogicalFile(level_ , logical_files_inputs_[i]->number);

	}
  // for (int which = 0; which < 2; which++) {
    // for (size_t i = 0; i < inputs_[which].size(); i++) {
      // edit->DeleteFile(level_ + which, inputs_[which][i]->number);
    // }
  // }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {

	return false;
  // Maybe use binary search to find right entry instead of linear search?
  // const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  // for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {//
    // const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    // for (; level_ptrs_[lvl] < files.size(); ) {
      // FileMetaData* f = files[level_ptrs_[lvl]];
      // if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // // We've advanced far enough
        // if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // // Key falls in this file's range, so definitely not base level
          // return false;
        // }
        // break;
      // }
      // level_ptrs_[lvl]++;
    // }
  // }
  // return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
      icmp->Compare(internal_key,
                    grandparents_[grandparent_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}

void Compaction::DoGroup(){
		const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
		int num=logical_files_inputs_.size();//number of the logical files
		//build iterator for the physical files of each logical_files_inputs_
		//std::vector<PhysicalMetaData>::iterator it[num];
		//PhysicalMetaData* advancer[num];
		//printf("vesion_set, num=%d\n", num);
		//exit(9);
		int advancer[num];
		int sizes[num];
		for(int i=0;i<num;i++){
			//advancer[i]= &(logical_files_inputs_[i]->physical_files[0]);
			
			advancer[i]=0;
			sizes[i]=logical_files_inputs_[i]->physical_files.size();
			//printf("vesion_set, size[i]=%d,file=%d\n", logical_files_inputs_[i]->physical_files.size(), logical_files_inputs_[i]->physical_files[0].number); 
		}		
		std::vector<PhysicalMetaData*>  participate_physical_files;
		participate_physical_files.clear();

		
		while(1){//push all the physical files to the participate_physical_files by their first key
			//PhysicalMetaData* next=NULL;//reset the next to null
			int next_indicator=-1;
			for(int i=0;i<num;i++){//for each logical file
				if(advancer[i] >= sizes[i]){//all physical files of this logical file has been pushed
					continue;
				}
				//here it[i] must not be null
				if(next_indicator==-1){//for the first time, next is null
					//next=it[i];
					next_indicator=i;
				}
				else{
					PhysicalMetaData *cur_visit= &(logical_files_inputs_[i]->physical_files[advancer[i]]);
					PhysicalMetaData *cur_next = &(logical_files_inputs_[next_indicator]->physical_files[advancer[next_indicator]]);
					if(user_cmp->Compare(cur_visit->smallest.user_key(),cur_next->smallest.user_key())<0){
						next_indicator=i;
					}
				}			
			}
			//next=it[next_indicator];
			//advance this iterater.
			//now we get the next smallest physical file
			if(next_indicator==-1){//all its are null
				break;
			}
			else {
				participate_physical_files.push_back(&( logical_files_inputs_[next_indicator]->physical_files[advancer[next_indicator]]) );//the physical file of the next_indicator.logical in poition of its advancer
			//it[next_indicator]++;
				advancer[next_indicator]++;
			}
			
		}
		//now we get the participate_physical_files.
		//printf("vesion_set, participate_physical_files size=%d\n", participate_physical_files.size());
		// for(int i=0;i<participate_physical_files.size();i++){
				// printf("file=%d, smallest=%s, largest=%s\n",participate_physical_files[i]->number, participate_physical_files[i]->smallest.user_key().data(), participate_physical_files[i]->largest.user_key().data());
		// }
			std::vector<PhysicalMetaData*> temp_group;
			temp_group.clear();
			//temp_group.push_back(participate_physical_files[0]);
			for(int i=0;i<participate_physical_files.size();i++){
				//before push this physical file, we check the temp_group to see if a new group should be split
				//if this physical file's smallest key is bigger than the largest key of the last file in temp_group, new group is split
				//that is, this physical file is not overlapped with the last file in temp_group.
				if(temp_group.size()!=0 && user_cmp->Compare(participate_physical_files[i]->smallest.user_key(),temp_group.back()->largest.user_key())>0 ){//a new group
					//printf("i=%d\n",i);
					groups.push_back(temp_group);
					temp_group.clear();
				}
				//no matter what, now we push this physical files to the temp_group.
				temp_group.push_back(participate_physical_files[i]);
				
			}

			//printf("temp_group size=%d, groups size=%d\n",temp_group.size(),groups.size());
			groups.push_back(temp_group);//push the last one
			//printf("temp_group size=%d, groups size=%d\n",temp_group.size(),groups.size());
			//
			//while(1)
		
		
		


}

}  // namespace leveldb
