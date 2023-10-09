#include "table/iterator_wrapper.h"


namespace leveldb {

	void IteratorWrapper::NextSSTTable() {
		iter_->NextSSTTable();
		Update();
	}

  void IteratorWrapper::Set(Iterator* iter) {
    delete iter_;
    iter_ = iter;
    if (iter_ == NULL) {
      valid_ = false;
    } else {
      Update();
    }
  }

  bool IteratorWrapper::Valid() const        {
		return valid_; 
  }
  Slice IteratorWrapper::key() const         {
		assert(Valid()); 
		//if(iter_ == NULL){
			//printf("iterator_wrapper.h, key, iter is NULL\n");
			//exit(9);
		//}		
		return key_;
		//return iter_->key();
	}
  void  IteratorWrapper::Next()  { 
		assert(iter_); 
		//fprintf(stderr,"iterator_wrapper.h, next\n");
		iter_->Next();        
		//fprintf(stderr,"iterator_wrapper.h, next,before update\n");
		Update(); 
		//fprintf(stderr,"iterator_wrapper.h, next,after update\n");
		
	}


void IteratorWrapper::Update() {
	//printf("iterator_wrapper.cc, update begin,iter_=%p\n",iter_);
    valid_ = iter_->Valid();
    if (valid_) {
      key_ = iter_->key();
    }
}
}