// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h> 
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h" 
#include "db/version_set.h"
#include <unistd.h>
#include <time.h>
#include <stdint.h>
#define s_to_ns 1000000000

extern int opened, closed;

double durations[100];//durations[1]=Write_duration
std::string dev_name;
struct timespec begin, end, stage; 
uint64_t write_total=0; 
uint64_t write_total_log=0; 
//extern  int kTargetFileSize; 
//int flash_using_exist;
//int sync_flag=0;
//int fill100k_flag=0;
char workload[1];

int read_method=1;
int read_thread=16;

int scan_amount=0;
long long read_mb(char *dev);

extern uint64_t diskTrafficBytes;
extern int fly[30];
extern int growth_factor;
extern uint64_t diskTrafficBytes;
extern uint64_t bytes_written_wa;
extern uint64_t bytes_input_wa;

int space_using(const char *dir) {
  FILE* fp;
  int res; 
  char command[128];
  sprintf(command,"du -lm %s | grep '[0-9]' ", dir);
  fp = popen(command, "r");
  
  char str[512];
  if ( fgets(str, sizeof(str), fp) != NULL ) {
    res = atoi(str);
  }
  fclose(fp);
  return res;
}

char result[200];
char* mem_using() {
  FILE* fp;
  int res = 0;
  int mem[3];
  int i = 0;
  int iPid = (int) getpid();

  char command[128];
  sprintf(command,"cat /proc/%d/status | grep -E 'VmSize|VmRSS|VmData' | grep -o '[0-9]\\+'", iPid);
  fp = popen(command, "r");

  char str[512];
  while ( fgets(str, sizeof(str), fp) != NULL && i < 3) {
    res = atoi(str);
    mem[i] = res/1024;
    ++i;
  }
  snprintf(result, sizeof(result), "VmSize: %6d MB VmRSS: %6d MB VmData: %6d MB",
          mem[0], mem[1], mem[2]);

  //sprintf(command, "free -h | grep Mem | awk '{print $3}'");
  //fp = popen(command, "r");
  //if( fgets(str, sizeof(str), fp) != NULL ) {
  //  ;
  //}
  snprintf(result, sizeof(result), "VmSize: %6d MB VmRSS: %6d MB VmData: %6d MB",
      mem[0], mem[1], mem[2]);
  //snprintf(result, sizeof(result), "VmSize: %6d MB VmRSS: %6d MB VmData: %6d MB Free (used): %s",
  //    mem[0], mem[1], mem[2], str);
  fclose(fp);
  return result; 
}

long long read_mb(char *dev) {
	FILE* fp;
	char command[128];
  long long res = 0;
  //sector, 512B
	sprintf(command, "cat /proc/diskstats | grep %s | awk '{print $8}'", dev);
	fp = popen(command, "r");
	char str[512];
  if ( fgets(str, sizeof(str), fp) != NULL) {
    res = atoi(str);
  }
  fclose(fp);
  res = res/1024/1024*512;
  return res;
}
// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)

static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "loadycsb"  //load ycsb
    "l,"       //load ycsb
    "ls,"      //load ycsn in sorted
    "a,"
    "b,"
    "c,"
    "d,"
    "e,"
    "f,"
    "xcompact,"	
    "overwrite,"
    "readrandom,"
    "readrandom,"  // Extra run to allow previous compactions to quiesce
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    ;

static int Flags_ratio = 0;

// Number of key/values to place in database
static uint64_t FLAGS_num = 1000000;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 1;// 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = true;

// Use the db with the following name.
static const char* FLAGS_db = NULL;

static const char* FLAGS_ycsb_path = NULL;

// stats logging every 1000000 ops 
static int FLAGS_stats_interval = 100000;

namespace leveldb {

namespace {

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) {
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

class Stats {
 private:
  double start_;
  double finish_;
  double seconds_;
  int64_t done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

 public:
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = Env::Default()->NowMicros();
    finish_ = start_;
    message_.clear();
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;

    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = Env::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = Env::Default()->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, ""); 
        fflush(stderr);
      }    
      last_op_finish_ = now; 
    }    
  
    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100; 
      else if (next_report_ < 5000)   next_report_ += 500; 
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stdout, "... finished %d ops%30s\r", done_, ""); 
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      double elapsed = (finish_ - start_) * 1e-6;
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);

    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str());
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
        rand(1000 + index) {
  }
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  uint64_t num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;
  FILE* stats_log_;

  void PrintHeader() {
    const int kKeySize = 16;
	 // printf("----------- I amd db_bench.cc , Run, before PrintEnvironment \n");
    PrintEnvironment();
	 //printf("------------ I amd db_bench.cc , Run, after PrintEnvironment \n");
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
	
	//printf("kTargetFileSize=%d MB\n", kTargetFileSize/1048576);
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    heap_counter_(0) {
    std::vector<std::string> files;
    Env::Default()->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        Env::Default()->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();
    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }
      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;
      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000; 
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("l")) {
        fresh_db = true;
        method = &Benchmark::LoadYCSB;
      } else if (name == Slice("a")||name == Slice("b")||name == Slice("c") ||
						name == Slice("d")||name == Slice("e")||name == Slice("f")||name == Slice("runycsb")) {
        method = &Benchmark::RunYCSB;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      }  else if (name == Slice("xcompact")) {
        fresh_db = false;
        method = &Benchmark::xCompact;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
		//sync_flag=1;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
		//fill100k_flag=1;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

	    if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }
      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
      }
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
	//printf("I amd db_bench.cc, ThreadBody, b  (arg->bm->*(arg->method))(thread)\n");
    (arg->bm->*(arg->method))(thread);
	//printf("I amd db_bench.cc, ThreadBody, a  (arg->bm->*(arg->method))(thread)\n");
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll();
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      Env::Default()->StartThread(ThreadBody, &arg[i]);
    }
    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }
    shared.start = true;
    shared.cv.SignalAll();
    while (shared.num_done < n) {
      shared.cv.Wait();
    }
    shared.mu.Unlock();
    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp();
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
	  //printf("_______ I amd db_bench.cc , Open, begin\n");
    assert(db_ == NULL);
    Options options;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
	//printf("_______ I amd db_bench.cc , Open, before DB::Open \n");
    Status s = DB::Open(options, FLAGS_db, &db_);
	//printf("_________ I amd db_bench.cc , Open, after DB::Open,s.ok()=%d \n",s.ok());
	//exit(1);//test if log is deleted  ---no
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1); //don't disable this line 
    }

    char path[200];
    snprintf(path, sizeof(path), "%s/%s", FLAGS_db, "stat.log");
    stats_log_ = fopen(path, "w+");
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  void WriteSeq(ThreadState* thread) {
	 // printf("I amd db_bench.cc, WriteSeq,begin\n");
	// exit(1);//test if log is deleted  --yes
    DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, false);
  }
  
 void xCompact(ThreadState* thread){
	printf("db_bench, xcompact!\n");
	exit(9);
} 
//----------------------------------------------------------------------------------  

void LoadYCSB(ThreadState* thread) {
	DoLoad(thread, false);
}

int get_fly(){

	int fly_sst=0;
	for(int i=0;i<10;i++){
		//printf("lev %d, fly %d\n",i, fly[i]);
		fly_sst+=  fly[i];
	}
	return fly_sst;

}
void DoLoad(ThreadState* thread, bool seq){
  if (num_ != FLAGS_num) {
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d ops)", num_);
    thread->stats.AddMessage(msg);
  }

	srand(time(NULL));

  RandomGenerator gen;
  WriteBatch batch;
  Status s;
  int64_t bytes = 0;
  char *path = "/home/lds/emlmt/workloads/myworkload-run-c-zipf-1G";
  FILE *load = fopen(path, "r");

	uint64_t i;
	uint64_t ops = 0;
  for ( i = 0; i < num_; i += entries_per_batch_) {
    batch.Clear();     
    char key[100];			
    if(seq){
      sprintf(key,"user%012d\0",i);
      //printf("i=%d,%s\n",i,key);				
    }
    else{
    /*
      char c;
      c=fgetc(load);
      if(c==EOF){
        printf("break in for\n");
        i=num_;					
        break;
        
      }
      //printf("%c\n",c);
      fgetc(load);//space, this is the type of the operations.
      fgets(key,100,load);
      
      key[strlen(key)-1]='\0';//regular the key, alter the newline to end of string
    */
      //if(i%10000000==0){
      //	printf("i=%d,%s\n",i,key);
      //}
      if( fgets(key,100,load) == NULL){
        printf("break in for\n");
        i=num_;					
        break;				
      }
      key[strlen(key)-1]='\0';//regular the key, alter the newline to end of string
      //printf("i=%d,%s\n",i,key);
    }
  
    value_size_=rand()%200+1;//vary the value size
    //printf("value size=%d\n",value_size_);
    batch.Put(key, gen.Generate(value_size_));
    bytes += value_size_ + strlen(key);
    //thread->stats.FinishedSingleOp();   
    s = db_->Write(write_options_, &batch);

    
    ops++;
    uint64_t rate=1000000;		
    //printf("size =%d\n", impl->versions_->current_->logical_files_[0].size());
    if(0==ops%rate){
      clock_gettime(CLOCK_MONOTONIC,&stage); 
      double stage_time=( (int)stage.tv_sec+((double)stage.tv_nsec)/s_to_ns ) - ( (int)begin.tv_sec+((double)begin.tv_nsec)/s_to_ns );
      PrintStats("leveldb.sstables");
      fflush(stats_log_);
    }
    
    if (!s.ok()) {
      fprintf(stderr, " I am db_bench.cc,  DoLoad, put error: %s\n", s.ToString().c_str());
      sleep(99999);
      exit(1);
    }
  }
	
	fclose(stats_log_);
	printf("write finished\n");
  thread->stats.AddBytes(bytes);
  }

int tcounter=0;//thread counter
pthread_mutex_t thread_counter_lock= PTHREAD_MUTEX_INITIALIZER;

void RunYCSB(ThreadState* thread) {
		//*******give each thread an identifier.
		ReadOptions options;
		std::string value;
	int loc_counter;//local thread identifier
	pthread_mutex_lock(&thread_counter_lock);
		tcounter++;
		loc_counter=tcounter;
		
		
		if(tcounter==1){
			printf("db_bench, readranom, invoke the thread pool\n");
			db_->Get(options, "0", &value).ok();//Invoke to create the thead pool.
			sleep(1);
			printf("db_bench, readranom, invoke the thread pool, done---------------------\n\n\n\n");
			
		}
	pthread_mutex_unlock(&thread_counter_lock);
	//*******give each thread an identifier.
	
	
	//char system[20];
	//strcpy(system, FLAGS_db);
	//char *tail=strchr(system+1,'/')+1;
    //printf("run ycsb,FLAGS_benchmarks=%s,system=%s\n",FLAGS_benchmarks,tail);
	
	char path[100];
	path[0]=0;
	
	printf("db_bench,RunYCSB,");   
	
	if(FLAGS_ycsb_path==NULL){
		printf("db_bench, please give the --ycsb_path=path_of_the_workload_dir\n"); 
		exit(9);
	}
	snprintf(path, sizeof(path), "%s/ycsb_%d.trace", FLAGS_ycsb_path, loc_counter-1);//FLAGS_ycsb_path is /home/workloads. The trace is nunmbered from 0~10.  
	
	//strcat(path,"/home/lds/emlmt/workloads/");
	//strcat(path, FLAGS_benchmarks);
	//strcat(path,"-run-1g-zipf.txt");
	
	printf("db_bench,RunYCSB,path=%s\n",path);
	
	FILE *workload=fopen(path,"r");
	printf("db_bench,RunYCSB,workload=%p\n",workload);
	
		if (num_ != FLAGS_num) {
		  char msg[100];
		  snprintf(msg, sizeof(msg), "(%d ops)", num_);
		  thread->stats.AddMessage(msg);
		}
	
		RandomGenerator gen;
		WriteBatch batch;
		Status s;
		int64_t bytes = 0;
		uint64_t find_times=0;
		uint64_t found = 0;
		uint64_t scan_times = 0;
		uint64_t scanned=0;
		
		uint64_t insert_times=0;
		uint64_t inserted=0;
	
		srand(time(NULL));
	
		printf("Run ycsb\n");
		
		int request=0;

		db_->Get(options, "0", &value).ok();//Invoke to create the thead pool.
		void *temp;
		//db_->Scan(options, "0", &temp, 0);//Invoke to create the scan thead pool.

		printf("\n\n\n\n\n\n\n");

		FILE *file=fopen("runycsb.txt","w+");
		clock_gettime(CLOCK_MONOTONIC,&begin);

		double total_us=0;	
		struct timespec begin2;
		clock_gettime(CLOCK_MONOTONIC,&begin2);

		while(1){
			//printf("db_bench, new request, request=%d\n",request);
			char command=fgetc(workload);
			if(command==EOF){
				printf("doycsb end\n");
				break;
			}
			
			char key[100];
			fscanf(workload," %s\n", key);
			//printf("command:%c|key:%s|\n",command,key);
//---------	read begin--------------------------------------------------------------------------	
			if(command=='r'){
				find_times++;
				//printf("key=%s\n",key);
				if (db_->Get(options, key, &value).ok()) {
					//printf("I am db_bench.cc, find it, ReadRandom, key=%s \n ", key);
					found++;
				}
				 else{
					  //printf("I am db_bench.cc, ReadRandom, not found,key=%s \n ", key);
				}
			}
//---------	read end--------------------------------------------------------------------------	
//--------- insert begin--------------------------------------------------------------------------
			else if(command=='u'||command=='i'){
				insert_times++;
				batch.Clear();
				batch.Put(key, gen.Generate(value_size_));
				bytes += value_size_ + strlen(key);
				//s = db_->Write(write_options_, &batch);
				
				inserted++;
			}
//----------insert end--------------------------------------------------------------------------
//----------scan begin--------------------------------------------------------------------------
			else if(command=='s'){
				
				int amount;
				amount=scan_amount;;//rand()%100;
				//printf("scan, %d numbers\n",amount);
				scan_times++;
			
				void *buf;
				//db_->Scan(options, key, &buf, amount);
				
				Iterator* iter = db_->NewIterator(ReadOptions());
				Slice startKey =Slice(key);
				iter->Seek(startKey);
				for(int i=0;i<amount;i++){
					if(iter->Valid()){
						scanned++;
						bytes += iter->key().size() + iter->value().size();
						iter->Next();
					}
					else{
						break;
					}
				}
				delete iter;
			}
//----------scan end--------------------------------------------------------------------------			
			//thread->stats.FinishedSingleOp();
			//c++;
			
			if (!s.ok()) {
				fprintf(stderr, " I am db_bench.cc,  do write, put error: %s\n", s.ToString().c_str());
				exit(1);
			}
			
			request++;
			//printf("db_bench, request=%d\n",request);
				int i=request;
			  if( i%100==0 ){//(reads_/10)
					clock_gettime(CLOCK_MONOTONIC,&stage); //begin insert , so begin counting
					double duration=( (int)stage.tv_sec+((double)stage.tv_nsec)/s_to_ns ) - ( (int)begin.tv_sec+((double)begin.tv_nsec)/s_to_ns );
					if(i%1000==0){//print in the screen, should be less frequent.
							//printf("front_th= %d , bg_th= %d , i= %d , found= %d , time= %f , used_mem= %d MB , read_io= %lld sectors\n",FLAGS_threads, read_thread,i,found,duration,mem_using(2),read_mb(recorder_name));
					}
					//fprintf(stats_log_,"front_th= %d , bg_th= %d , i= %d , found= %d , time= %f , used_mem= %d MB , read_io= %lld sectors\n",FLAGS_threads, read_thread,i,found,duration,mem_using(2),read_mb(recorder_name));
					fflush(stats_log_);
			  }		
				
			if(request==num_){
        // printf("key=%s, durations[50]=%f\n", durations[50],key);
        // printf("key=%s, durations[51]=%f\n", durations[51],key);//block.cc Seek()
        // printf("key=%s, durations[52]=%f\n", durations[52],key);//block.cc  Decode
        break;
			}
			
		}

		double count_time=( (int)stage.tv_sec+((double)stage.tv_nsec)/s_to_ns ) - ( (int)begin2.tv_sec+((double)begin2.tv_nsec)/s_to_ns );

		printf("db_bench, end, count_time=%f us, total_time=%f us\n", count_time*1000, total_us*1000);

		fflush(file);
		fclose(file); 
	
		thread->stats.AddBytes(bytes);
		 
		printf("find:%d/%d, scan:%d/%d,insert:%d/%d\n",found, find_times,scanned,scan_times,inserted,insert_times);
		printf("db_bench, at env_posix.cc, durations[70]=%f\n",durations[70]);
		printf("db_bench, at env_posix.cc, durations[71]=%f\n",durations[71]);

		//printf("advised/read=%f\n",durations[60]);
  }
//----------------------------------------------------------------------------------  
void print_write_tail() {
}

void DoWrite(ThreadState* thread, bool seq) {
  if (num_ != FLAGS_num) {
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d ops)", num_);
    thread->stats.AddMessage(msg);
  } 

  RandomGenerator gen;
  WriteBatch batch;
  Status s;
  uint64_t now_s, end_s;
  float w_amp;
  char* mem_use;
  int dir_size;
  int64_t bytes = 0;

  now_s = leveldb::Env::Default()->NowMicros();

  for (int i = 0; i < num_; i += entries_per_batch_) {
    batch.Clear();
    for (int j = 0; j < entries_per_batch_; j++) {
      const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
      char key[100];
      snprintf(key, sizeof(key), "%016d", k);
      batch.Put(key, gen.Generate(value_size_));
      bytes += value_size_ + strlen(key);
      thread->stats.FinishedSingleOp();
      //fprintf(stats_log_, "key: %s \n", key);
    }
        
    s = db_->Write(write_options_, &batch);
      
    if(0 == i % FLAGS_stats_interval) {
      end_s = Env::Default()->NowMicros();
      dir_size = space_using(FLAGS_db);
      w_amp = (bytes_input_wa == 0) ? 0.0 : (float)bytes_written_wa/bytes_input_wa;
      mem_use = mem_using();
      // printf("stage_time: %010d total_time= %10.1f s  wa: %3.1f w_amp: %3.1f %s space: %10d MB\n",
      //         i, (float)(end_s - now_s)/1000000.0, 
      //         (float)diskTrafficBytes/bytes, w_amp, 
      //         mem_use, dir_size);
      // fprintf(stats_log_, "stage_time: %010d total_time: %10.1f s  wa: %3.1f w_amp: %3.1f %s space: %10d MB\n",
      //         i, (float)(end_s - now_s)/1000000.0, 
      //         (float)diskTrafficBytes/bytes, w_amp, 
      //         mem_use, dir_size);
      PrintStats("leveldb.sstables");
    }
        
    if (!s.ok()) {
      fprintf(stderr, "put error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  fclose(stats_log_);
  thread->stats.AddBytes(bytes);
}

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      //thread->stats.FinishedSingleOp();
      ++i;
      //fprintf(stats_log_, "key: %s \n", iter->key());
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }
 
void ReadRandom(ThreadState* thread) {
  ReadOptions options;
  std::string value;
  int found = 0;
  for (int i = 0; i < reads_; i++) {
    char key[100];
		const int k = thread->rand.Next() % FLAGS_num;
    snprintf(key, sizeof(key), "%016d", k);
    if (db_->Get(options, key, &value).ok()) {
      fprintf(stats_log_, "ReadRandom Found key: %s\n", key);
      found++;
    } else {
      fprintf(stats_log_, "ReadRandom NotFound key: %s\n", key);
	  }
    //thread->stats.FinishedSingleOp();
  } 
  char msg[100];
  snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
  thread->stats.AddMessage(msg);
#if 0
  ReadOptions options;
  std::string value;
  int found = 0;
	
  //*******give each thread an identifier.
	int loc_counter;//local thread identifier
	int lock_res= pthread_mutex_lock(&thread_counter_lock);
  if(lock_res!=0) {
    printf("db_bench, ReadRandom,lock_res=%d\n",lock_res);
    exit(9);
  }

  tcounter++;
  //printf("db_bench, readrandom, tcounter=%d\n",tcounter);
  
  if(tcounter==1) {
    printf("db_bench, readranom, invoke the thread pool\n");
    db_->Get(options, "0", &value).ok();//Invoke to create the thead pool.
    sleep(1);
    printf("db_bench, readranom, invoke the thread pool, done---------------------\n\n\n\n");
  }
  //printf("db_bench, readrandom, tcounter=%d\n",tcounter);

  loc_counter=tcounter;
  //printf("db_bench, readrandom, loc_counter=%d, begin sleep\n",loc_counter);
	pthread_mutex_unlock(&thread_counter_lock);
  //*******give each thread an identifier.
	
	clock_gettime(CLOCK_MONOTONIC,&begin); 
    for (int i = 0; i < reads_; i++) {
      char key[100];
	  int next_rand=thread->rand.Next();
		
		//const int k = next_rand % FLAGS_num;//I change this 
		const int k = next_rand;
      //const int k = thread->rand.Next() % FLAGS_num;
	  //printf("k=%d\n",k);
      snprintf(key, sizeof(key), "%016d", k);
	  //printf("I am db_bench.cc, ReadRandom, before Get, i=%d, k=%d\n",i, k);
      if (db_->Get(options, key, &value).ok()) {
		 // printf("I am db_bench.cc, ReadRandom, i=%d, k=%d ,FLAGS_num=%d \n",i, k,FLAGS_num);
		found++;
      }
	  else{
		  //printf("I am db_bench.cc, ReadRandom, not found, i=%d, k=%d ,FLAGS_num=%d \n",i, k,FLAGS_num);
	  }
	  
	 //printf("I am db_bench.cc, ReadRandom, after Get, i=%d, k=%d, th=%d\n",i, k,loc_counter);
	   //printf("---------------------------------- \n");
	  
	  if( i%10==0 &&i!=0){//(reads_/10)
			clock_gettime(CLOCK_MONOTONIC,&stage); //begin insert , so begin counting
			double duration=( (int)stage.tv_sec+((double)stage.tv_nsec)/s_to_ns ) - ( (int)begin.tv_sec+((double)begin.tv_nsec)/s_to_ns );
			if(i%10000==0){//print in the screen, should be less frequent.
					//printf("front_th= %d , bg_th= %d , i= %d , found= %d , time= %f , buffer/cache= %d MB , read_io= %lld sectors\n",FLAGS_threads, read_thread,i,found,duration,mem_using(4),read_mb(recorder_name));
			// fprintf(recorder,"front_th= %d , bg_th= %d , i= %d , found= %d , time= %f , buffer/cache= %d MB , read_io= %lld sectors\n",FLAGS_threads, read_thread,i,found,duration,mem_using(4),read_mb(recorder_name));
			// fflush(recorder);
			}
	  }
      //thread->stats.FinishedSingleOp();
    }
	
	clock_gettime(CLOCK_MONOTONIC,&stage);
	double duration=( (int)stage.tv_sec+((double)stage.tv_nsec)/s_to_ns ) - ( (int)begin.tv_sec+((double)begin.tv_nsec)/s_to_ns );
	printf("db_bench,readrandom, finish, dur=%f s, th=%d\n",loc_counter, duration);
	
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
	//sleep(4);
#endif
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d.", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      snprintf(key, sizeof(key), "%016d", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      snprintf(key, sizeof(key), "%016d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        batch.Delete(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

		
        const int k = thread->rand.Next() % FLAGS_num; 
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) {
  
	printf("db_bench, Compact\n");
	exit(9);
    //db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stats_log_, "\n%s\n", stats.c_str());
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = Env::Default()->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      Env::Default()->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb 

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      //force to read_only
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
	} else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (sscanf(argv[i], "--gf=%d%c", &n, &junk) == 1) {
      growth_factor = n;
    } else if (sscanf(argv[i], "--rmethod=%d%c", &n, &junk) == 1) {
      read_method = n;
    }else if (sscanf(argv[i], "--rthread=%d%c", &n, &junk) == 1) {
      read_thread = n;
    }else if (sscanf(argv[i], "--samount=%d%c", &n, &junk) == 1) {
      scan_amount = n;
    }else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    }else if (strncmp(argv[i], "--ycsb_path=", 12) == 0) {
      FLAGS_ycsb_path = argv[i] + 12;
    }  else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}
