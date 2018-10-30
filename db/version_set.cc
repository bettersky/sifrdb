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

// Find the physical file that possiblely includes the key,  in the given logical file
int FindFile(const InternalKeyComparator& icmp,
			 const LogicalMetaData& logical_file,
             const Slice& key) {
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
        index_(flist->physical_files.size()) {
  }

	virtual Slice currentSSTLargestKey() {
		return flist_->physical_files[index_].largest.Encode();
	}
	virtual Slice currentSSTSmallestKey() {
		return flist_->physical_files[index_].smallest.Encode();
	}

	virtual Slice nextSSTSmallestKey() {
		printf("versionset.cc, nextSSTSmallestKey\n");
	}

	PhysicalMetaData* GetSSTTableMeta() {
    //printf("%d\n", index_);
    //printf("%d\n", flist_->physical_files[index_].smallest.Encode());
		return const_cast<PhysicalMetaData*>(&flist_->physical_files[index_]);
	}

  virtual bool Valid() const {
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
  const LogicalMetaData *flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
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

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
	for (int level = 0; level < config::kNumLevels; level++) {
		for (int i = 0; i < logical_files_[level].size(); i++) {
			iters->push_back(
				NewTwoLevelIterator(new LogicalSSTNumIterator(
          vset_->icmp_, logical_files_[level][i]), &GetFileIterator, vset_->table_cache_, options));
		}
	}
}

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;

  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    // printf("parsed_key: %s\n", parsed_key.user_key);
    // printf("saver user_key: %s\n", s->user_key);
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(PhysicalMetaData* a, PhysicalMetaData* b) {
  return a->number > b->number;
}

struct timespec search_begin, search_stage;

int scan_th_counter=0;

int th_counter=0;
void Version::SearchThread() {
  mutex_for_searching_queue.Lock();
  uint32_t th_id=th_counter++;
  mutex_for_searching_queue.Unlock();

	printf("versionset.cc, Forest_back, thread created, searching_right=%p, th_id=%d\n",searching_right,th_id);
	while (true) {
		Searching_item *to_search;

    mutex_for_searching_queue.Lock();
    int current_id; // determined by the search id of the selected logical file

    while(1) {
      int found_logical_file_to_seacrh=0;
      //get an SST to search begin
      to_search = searching_right;
      //printf("versionset, forest_back, 1111,to_search=%p\n",to_search);
      while (to_search != NULL && 
             (to_search->should_pop==1 || to_search->logical_file_counter >= logical_file_total) ){
          //This item is not needed to be searched, so we move to the later item (the previous one).
        to_search= to_search->pre;
      }
      //Coming to here, either the item is NULL, or it is waiting to be serviced. That is, to_search->should_pop is 0, and logical_file_counter < logical_file_total
      if (to_search == NULL) {  //Searching queue can be regarded as empty.
        //printf("versionset, forest_back, to_search=%p\n",to_search);
        found_logical_file_to_seacrh=0;
      } else if ( to_search->should_pop ==0 && to_search->logical_file_counter < logical_file_total) {
        // Found an SST to search. To perform the search operation.
        found_logical_file_to_seacrh = 1;
      } else {
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

      } else if (1==found_logical_file_to_seacrh){//found an sst to search
        break;
      }

      printf("versionset, error,111111111 exit\n");
      exit(9);
    }

    current_id=to_search->logical_file_counter;
    to_search->logical_file_counter++;//the next thread will search the next logical file.//the next thread will search the next logical file.
    LogicalMetaData *logical_f = logical_files_set[current_id];
    ReadOptions &options_local = to_search->options;

    char data_for_searching_ikey[100];//Assume that the key size is less than 100 bytes.
    memcpy(data_for_searching_ikey, to_search->ikey.data_ , to_search->ikey.size());//This is important!
    Slice ikey_local=Slice(data_for_searching_ikey,  to_search->ikey.size());//Make the searching_ikey local

    Saver saver_;
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
		if (saver_.ucmp->Compare(saver_.user_key, logical_f->smallest.user_key()) >= 0 
        &&saver_.ucmp->Compare(saver_.user_key, logical_f->largest.user_key()) <= 0) {
			// range search.
			uint32_t index = FindFile(vset_->icmp_, *logical_f, ikey_local);
			if (index >= logical_f->physical_files.size()) {
							//do nothing. no physical file responses to this key.
				//saver_.state=kNotFound;
			} else if (saver_.ucmp->Compare(saver_.user_key, logical_f->physical_files[index].smallest.user_key()) >= 0) {
        // find a physical file
        PhysicalMetaData *physical_f= &(logical_f->physical_files[index]);//
        if (saver_.ucmp->Compare(saver_.user_key, physical_f->smallest.user_key())< 0 ||
          saver_.ucmp->Compare(saver_.user_key, physical_f->largest.user_key()) > 0) {
          // Not contained in this physical file.
          // saver_.state=kNotFound;
        } else {  // Possibely contained in this SST file.
          uint64_t physical_file_number= physical_f->number;
          uint64_t physical_file_size= physical_f->file_size;
          s = vset_->table_cache_->Get(options_local, physical_file_number, physical_file_size, ikey_local, &saver_, SaveValue);
        }
			}
		}
		else{//The key is not in the range of the logical file, so do nothing.
			//saver_.state=kNotFound;
		}

    // Process the search results  begin
		mutex_for_searching_queue.Lock();
		//printf("version_set.cc,check begin, th_id=%d\n",th_id);
    int should_grant_value=0;
    if (to_search == NULL) {
      //There is possibility that the item is deleted by the front thread.

    } else if (to_search->should_pop == 1 ||
               fingerprint != to_search->fingerprint || 
               to_search->ikey!= ikey_local ) {
      //The should_pop has been set by another thread, or the fingerprint is not the one of to_search. There is no need to check.
      //printf("version_set.cc,  con search, to_search=null\n");
      //exit(9);
    } else { //***********checking and operating begin****************
      int check_num=logical_file_total;//default to check all results.

      if (kFound == saver_.state) {
        //printf("version_set.cc, kFound\n");
        to_search->search_result_of_ssts[current_id]=2;//Mark the id for this result as Found.
        check_num=current_id;//If the key is found in this thread, all later results need not to be checked
        to_search->found_flag=1;//This is a bug, but now we use it because we do not consider deletions.
        should_grant_value=1;
      } else {
        to_search->search_result_of_ssts[current_id]=1;//Mark the id for this result as NotFound.
      }

      int should_pop_local = 1; //Defaulting the flag to be 1.

      for (int i = 0; i < check_num; i++) {
        if (0==to_search->search_result_of_ssts[i]) {
          // The logical file of this id has not been searched, so at least the thread that searches this logcial file will response the final check.	//end the for, and not pop
          should_pop_local=0;
          break;
        }
        else if (2==to_search->search_result_of_ssts[i]) {
          // All the front results must be one, saying that they have finished the searches, so this result of Found is the final result and we can conclude the search.
          should_pop_local=1;
          should_grant_value=0;//There is more sound result has granted the value.
          break;
        }
      }

      if (should_grant_value == 1) {
        to_search->value=saver_.value;
      }
      //Note that if the current result is one, the threads will check all the results. And if all the results are one, this thread response for the pop.

      if (should_pop_local == 1) {
        //printf("version_set.cc, get end, pop, item->found_flag=%d\n",item->found_flag);
        //cv_for_delaying.SignalAll();//we use spin lock in the main thread.
        to_search->should_pop=1;//Marks so that the main thread returns the result.
        if (to_search->found_flag ==1 ) {
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

Status Version::Get(const ReadOptions& options,
                    const LookupKey& k,
                    std::string* value,
                    GetStats* stats) {
#ifdef SEARCH_PARALLEL
  if (create_thread_.NoBarrier_Load() == NULL) {
    for (int i = 0; i < NUM_READ_THREADS; ++i) {
      Env::Default()->StartThread(&Version::SearchWrapper, this);
    }
  }
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  Searching_item *new_item= new Searching_item();
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
  if(searching_right==NULL) {
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
  if(searching_right==NULL){  //The last item is deleted, and searching_left also point to that item.
    searching_left=NULL;
  }
  queue_size--;
  mutex_for_searching_queue.Unlock();

  int status=temp->final_state;
  value=temp->value;
  delete temp;
	if (status == 1) {
		return Status::OK();
	} else return Status::NotFound(Slice());

#else
#ifdef READ_PARALLEL
  pthread_t current_thread = vset_->env_->GetThreadId();
#endif
  Slice ikey = k.internal_key();
  Slice user_key = k.user_key();
  const Comparator* ucmp = vset_->icmp_.user_comparator();
  Status s;

  stats->seek_file = NULL;
  stats->seek_file_level = -1;
  int last_file_read_level = -1;

  // We can search level-by-level since entries never hop across
  // levels.  Therefore we are guaranteed that if we find data
  // in an smaller level, later levels are irrelevant.

  // to do range search for getting the ssts that possibly hold the key
	// we should push the physical files to the tmp for search
	std::vector<PhysicalMetaData*> tmp; // to hold the files to be search
  for (int level = 0; level < config::kNumLevels; level++) {
		tmp.clear();
		size_t num_logical_files = logical_files_[level].size();
		if (num_logical_files == 0) continue;

		// Get the list of files to search in this level
		LogicalMetaData* const* logical_files_in_level = &logical_files_[level][0];

    // Each logical file at most have one physical responsive physical file
		tmp.reserve(num_logical_files);
		// may need to make sure the logical files in the level are sorted by time.
		for (uint32_t i = 0; i < num_logical_files; i++) {
      // get the physical files may be contain the keys.
			LogicalMetaData* logical_f = logical_files_in_level[i];
			if (ucmp->Compare(user_key, logical_f->smallest.user_key()) >= 0 &&
          ucmp->Compare(user_key, logical_f->largest.user_key()) <= 0) {
				// within logical range.
				uint32_t index = FindFile(vset_->icmp_, *logical_f, ikey);
				if (index >= logical_f->physical_files.size()) {
					// do nothing. no physical file responses to this key.
				} else if (ucmp->Compare(user_key, logical_f->physical_files[index].smallest.user_key()) >= 0) {
          // TODO: 不比较largest的原因是二分查找，大的key，index会大    find a physical file
					tmp.push_back(&(logical_f->physical_files[index]));   // tmp contains pointers
				}
			}
		}

		if (tmp.empty()) continue;  //continue to search the next level

    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    //std::reverse(tmp.begin(), tmp.end());
		PhysicalMetaData* const* physical_files = &tmp[0]; //points to the vector of files.
		int num_physical_files = tmp.size();
#ifndef READ_PARALLEL
	  // here we get the files to search for the current level.
    for (uint32_t i = 0; i < num_physical_files; ++i) {
      // begin to read the files by order. However, in lsm-forest here will be changed to concurrently.
      PhysicalMetaData* f = physical_files[i];

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
          break;      // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }
#else
    for (uint32_t i = 0; i < num_physical_files; ++i) {
      SearchItem* item = new SearchItem();
      // Saver* saver = new Saver();
      // saver->state = kNotFound;
      // saver->ucmp = ucmp;
      // saver->user_key = user_key;
      // saver->value = value;
      Saver saver;
      saver.state = kNotFound;
      saver.ucmp = ucmp;
      saver.user_key = user_key;
      saver.value = value;

      item->ikey = ikey;
      item->options = options;
      item->saver = saver;
      item->id = current_thread;
      item->file = tmp[i];

      vset_->iQueue_.Push(*item);
    }

    std::vector<SearchItem> sv;
    for (uint32_t i = 0; i < num_physical_files; ++i) {
      SearchItem* item;
      vset_->mapQueue_[current_thread].Pop(item);
      sv.push_back(*item);
    }

    for (uint32_t i = 0; i < num_physical_files; ++i) {
      auto item = sv[i];
      Status s = item.status;
      Saver saver = item.saver;
      if (!s.ok()) {
        return s;
      }
      switch (saver.state) {
        case kNotFound:
          continue;      // Keep searching in other files
        case kFound:
          return s;
        case kDeleted:
          s = Status::NotFound(Slice());  // Use empty error message for speed
          return s;
        case kCorrupt:
          s = Status::Corruption("corrupted key for ", user_key);
          return s;
      }
    }


#endif
  } // end searching a level, may be have found and returned
#endif
  // end all the searching and no key found
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

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    const std::vector<LogicalMetaData*>& logical_files = logical_files_[level];
    size_t logical_num = logical_files.size();
    if (logical_num == 0) continue;
    // level
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    // logical_num
    r.append("num_logical: ");
    AppendNumberTo(&r, logical_num);
    r.append("\n");

    for (size_t i = 0; i < logical_num; i++) {
      const std::vector<PhysicalMetaData>& files = logical_files[i]->physical_files;
      r.append("Logical file: \n");
      r.append("        ");
      AppendNumberTo(&r, logical_files[i]->number);
      r.append(":");
      AppendNumberTo(&r, logical_files[i]->file_size);
      r.append("[");
      r.append(logical_files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(logical_files[i]->largest.DebugString());
      r.append("]\n");
      // AppendNumberTo(&r, i);
      // r.append(" ---\n");
      // r.append(" files: ");
      // AppendNumberTo(&r, files.size());
      // r.append(", ");
      r.append("Physical file: ");
      AppendNumberTo(&r, files.size());
      r.append("\n");
#if 0
      for (size_t i = 0; i < files.size(); i++) {
        r.append("        ");
        AppendNumberTo(&r, files[i].number);
        r.push_back(':');
        AppendNumberTo(&r, files[i].file_size);
        r.append("[");
        r.append(files[i].smallest.DebugString());
        r.append(" .. ");
        r.append(files[i].largest.DebugString());
        r.append("]\n");
      }
#endif
    }
    //r.append("]\n");
  }
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
  //  edit contains added logical files and deleted logical files
  void Apply(VersionEdit* edit) {
    // Delete files
    const VersionEdit::DeletedLogicalFileSet& del = edit->deleted_logical_files_;
    for (VersionEdit::DeletedLogicalFileSet::const_iterator iter = del.begin();
         iter != del.end(); ++iter) {
      const int level = iter->first;
      const uint64_t number = iter->second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
	  for (int i = 0; i < edit->new_logical_files_.size(); i++) {
      const int level = edit->new_logical_files_[i].first;
      LogicalMetaData* f = new LogicalMetaData(edit->new_logical_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);  //erase to not delete the file
      levels_[level].added_files->insert(f);
	}
}

  // Save the current state in *v.
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // I think here is only one file of one level, so we can optimize it.
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<LogicalMetaData*>& base_files = base_->logical_files_[level];
      std::vector<LogicalMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<LogicalMetaData*>::const_iterator base_end = base_files.end();

      const FileSet* added = levels_[level].added_files;
      v->logical_files_[level].reserve(base_files.size() + added->size());
      // add the base files TODO
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

      // add the new file
      // assure that the size of added is 1.
      for (FileSet::const_iterator added_iter = added->begin(); added_iter != added->end(); ++added_iter) {
        // FileSet::const_iterator added_iter = added->begin();
        MaybeAddFile(v, level, *added_iter);
      }

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
  }

  void MaybeAddFile(Version* v, int level, LogicalMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<LogicalMetaData*>* files = &v->logical_files_[level];
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
#ifdef READ_PARALLEL
  for (int i = 0; i < NUM_READ_THREADS; ++i) {
    Env::Default()->StartThread(&VersionSet::SearchWrapper, this);
  }
#endif
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
		// This means that we have already calculated the bloom filter for this file and files
    //  are immutable (wrt a file number)
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

void VersionSet::Generate_file_level_bloom_filter() {
  Log(options_->info_log, "Recovering Generate_file_level_bloom_filter begin");
  //If the file named "file_level_bloom_filter" does not exist, generate the file.
  //Else, read the file to generate filter.
  filter_file = fopen("file_level_bloom_filter", "w+");

  const FilterPolicy *filter_policy = options_->filter_policy;
  FileLevelFilterBuilder file_level_filter_builder(filter_policy);
  if (filter_policy == NULL) {
    Log(options_->info_log, "Generate_file_level_bloom_filter, filter_policy is NULL, do not Generate_file_level_bloom_filter");
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
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
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
  }

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

  return s;
}

int VersionSet::Test(){
	for(int i=0;i<10;i++){
		printf("version_set.cc, Lev %d, logical num %d\n",i, current_->logical_files_[i].size());

	}
}
Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);

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
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true/*checksum*/, 0/*initial_offset*/);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

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

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version

    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;
  }
  return s;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?
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
    for (size_t i = 0; i < files.size(); i++) {
      const LogicalMetaData* f = files[i];
      edit.AddLogicalFile(level, *f);
    }
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

// Num Level Logical Files 
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
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_; v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<LogicalMetaData*>& logical_files = v->logical_files_[level];
      for (size_t i = 0; i < logical_files.size(); i++) {
        for (size_t j = 0; j < logical_files[i]->physical_files.size(); j++) {
          live->insert(logical_files[i]->physical_files[j].number);
        }
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->logical_files_[level]);
}

#ifdef READ_PARALLEL
void VersionSet::SearchThread() {
  SearchItem item;

  while (iQueue_.Pop(&item)) {
    PhysicalMetaData* f;
    Saver saver;
    Slice ikey = item.ikey;
    ReadOptions options;
    Status s;
    f = item.file;
    saver = item.saver;
    options = item.options;

    s = table_cache_->Get(options, f->number, f->file_size, ikey, &saver, SaveValue);
    item.status = s;
    mapQueue_[item.id].Push(item);
  }
}
#endif

Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap

  //number of iterators
	const int space =  c->logical_files_inputs_.size();
  Iterator** list = new Iterator*[space];
  int num = 0;

  for (int i = 0; i < space; i++) {
    list[num++] = NewTwoLevelIterator(
        new Version::LogicalSSTNumIterator(icmp_, c->logical_files_inputs_[i]),
        &GetFileIterator, table_cache_, options);
  }

  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

bool VersionSet::NeedsCompaction(bool* locked, int& level) {
  for (int i = 1; i < config::kNumLevels; ++i) {
    if (locked[i]) {
      continue;
    }
    if (current_->logical_files_[i].size() > growth_factor &&
        current_->logical_files_[i + 1].size() <= growth_factor*1.5) {
      Log(options_->info_log, "Level-%d needs Compaction\n", i);
      level = i;
      return true;
    }
  }

  if (!locked[0] && current_->logical_files_[0].size() > growth_factor) {
    level = 0;
    Log(options_->info_log, "Level-0 needs Compaction\n");
    return true;
  }
  return false;
}

Compaction* VersionSet::PickCompaction(int level) {
  assert(level >= 0);
  assert(level+1 < config::kNumLevels);
  Compaction* c = new Compaction(level);

  // 我们可能想先选择较老的10个文件
  for (int i = 0; i < growth_factor; i++) {
    LogicalMetaData* f = current_->logical_files_[level][i];
    c->logical_files_inputs_.push_back(f);
  }
  if (c->logical_files_inputs_.empty()) {
    // Wrap-around to the beginning of the key space
    printf("versionset, pick, wrap back\n");
    exit(1);
  }
	c->input_version_ = current_;
	c->input_version_->Ref();
	return c;
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

void Compaction::AddInputDeletions(VersionEdit* edit) {
	for (int i = 0; i < logical_files_inputs_.size(); i++) {
		edit->DeleteLogicalFile(level_, logical_files_inputs_[i]->number);
	}
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
}  // namespace leveldb
