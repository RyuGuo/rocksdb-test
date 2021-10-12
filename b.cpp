/**
 * 测试RocksDB并发写入
 * * 多线程写入
 * * 多实例写入
 */

#include <iostream>
#include <ctime>
#include <sys/time.h>
#include <pthread.h>
#include <vector>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <atomic>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/threadpool.h>

using namespace std;
using namespace rocksdb;

#define META_N 1000000000LL
#define THREAD_N 16
#define DB_N 8
#define CF_N 1
#define WAL_OFF 0
#define KEY_GEN_LOCAL 1

#define KEY_SIZ 16
//metadata size
#define META_SIZ 128
//chunk size
#define CHUNK_SIZ 8

/**
 * 0 - 单实例分hash片写入
 * 1 - 多实例写入
 * 4 - 多列族写入
 */
#define METHOD 4
#if METHOD == 0
#undef DB_N
#define DB_N 1
#undef CF_N
#define CF_N 1
#elif METHOD == 1
#undef CF_N
#define CF_N 1
#elif METHOD == 4
// #undef DB_N
// #define DB_N 1
#endif // METHOD

#define PRINT_INTERVAL_N (META_N / 20)
#define MAX(a, b) (((a) > (b)) ? (a) : (b))

struct GenerateKeyParam;

struct CTX {
  char *data_ptr;
  vector<pair<string, char*>> *v;
  int id;
  atomic<uint64_t> *run_count_ptr;
  struct timeval *start;
  string (*LocalGenerateKey)(GenerateKeyParam &&arg);
  int (*keyDistribute)(string &key);
  int (*DBDistribute)(string &key);
  int (*CFDistribute)(string &key);
};

CTX ctxs[THREAD_N * DB_N];
vector<pair<string, char*>> metadatas[THREAD_N];
vector<pair<string, char*>> chunkdatas[THREAD_N];
DB *dbs[DB_N];
Options options;
WriteOptions write_options;
ReadOptions read_options;
BlockBasedTableOptions table_options;
ColumnFamilyOptions cf_options(options);
vector<ColumnFamilyHandle*> handles[DB_N];

char *metadata_data;
char *chunk_data;
int fds[THREAD_N];

void openWal(int id)
{
  fds[id] = open(("kv/wal" + to_string(id)).c_str(), O_CREAT | O_RDWR);
}
void wal(int id, pair<string, char*> &p)
{
  string s = p.first + p.second;
  // assert(write(fds[id], s.c_str(), s.size()) > 0);
}
//rdtsc generator
inline unsigned long rdtsc(void){
  union {
    unsigned long tsc_64;
    struct {
      unsigned lo_32;
      unsigned hi_32;
    };
  } tsc;
  asm volatile("rdtsc" :
               "=a" (tsc.lo_32),
               "=d" (tsc.hi_32));
  return tsc.tsc_64;
}
#define CPU_MHZ 2300L
inline void _usleep(unsigned long n){
  unsigned long st, d;
  d = n * CPU_MHZ;  //2.2~2.3GHz
  st = rdtsc();
  while(rdtsc() - st < d);
}
struct GenerateKeyParam {
    uint64_t total;
    uint64_t i;
    int piece;
    int unique_var;
};
inline string GenerateKey(GenerateKeyParam &&arg) {
    const string ke = ("%0" + to_string(KEY_SIZ) + "d");
    char key[KEY_SIZ];
    sprintf(key, ke.c_str(), arg.i);
    return string(key, KEY_SIZ);
}
inline string GenerateLinearKey(GenerateKeyParam &&arg) {
  const string ke = ("%0" + to_string(KEY_SIZ) + "d");
  char key[KEY_SIZ];
  sprintf(key, ke.c_str(), arg.i * arg.piece + arg.unique_var);
  return string(key, KEY_SIZ);
}
inline string GenerateBlockLinearKey(GenerateKeyParam &&arg) {
  const int R = 1000;
  const string ke = ("%048d%032d");
  char key[KEY_SIZ] = {0};
  int b_idx = arg.i % R;
  int i_idx = (arg.i / R);
  int ii_idx = (i_idx + b_idx) % R;
  sprintf(key, ke.c_str(), b_idx, (ii_idx * arg.total / R + i_idx) * arg.piece + arg.unique_var);
  return string(key, KEY_SIZ);
}
inline string GenerateHashKey(GenerateKeyParam &&arg) {
  uint64_t i = arg.i;
  struct {
    uint64_t pre;
    uint64_t las;
    uint8_t reserve[KEY_SIZ - 2 * 8];
  } _h = {
    hash<uint64_t>()(i),
    (hash<uint64_t>()((i << 7) ^ (i >> 25)) << 8) | hash<uint8_t>()(arg.unique_var)
  };
  return string((char*)&_h, sizeof(_h));
}
inline int keyDistribute(string &key) {
  return hash<string>{}(key) % THREAD_N;
}
inline int DBDistribute(string &key) {
  return hash<string>{}(key) % DB_N;
}
inline int CFDistribute(string &key) {
  return hash<string>{}(key) % CF_N;
}
inline int DBDistribute_among_Meta_chunk(string &key) {
  return hash<int>{}((key[0] == '0') ? 0 : 1) % DB_N;
}
inline int CFDistribute_among_Meta_chunk(string &key) {
  return hash<int>{}((key[0] == '0') ? 0 : 1) % CF_N;
}
void do_write(DB *db, pair<string, char*> &p, int id, ColumnFamilyHandle *handle)
{
  Status status;
#if METHOD == 0
  status = db->Put(write_options, handle, to_string(id % DB_N) + p.first, p.second);
#else
  status = db->Put(write_options, handle, p.first, p.second);
#endif // METHOD
#if WAL_OFF == 1
  wal(id, p);
#endif // WAL_OFF
  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.getState());
    assert(status.ok());
  }
}
void do_read(DB *db, pair<string, char*> &p, int id, ColumnFamilyHandle *handle)
{
  Status status;
  string res;
#if METHOD == 0
  status = db->Get(read_options, to_string(id % DB_N) + p.first, &res);
#else
  status = db->Get(read_options, handle, p.first, &res);
#endif
  if (!status.ok()) {
    fprintf(stderr, "%s\n", status.getState());
    assert(status.ok());
  }
}
void do_delete(DB *db, pair<string, char*> &p, int id, ColumnFamilyHandle *handle)
{
#if METHOD == 0
  db->Delete(write_options, handle, to_string(id % DB_N) + p.first);
#else
  db->Delete(write_options, handle, p.first);
#endif // METHOD
#if WAL_OFF == 1
  wal(id, p);
#endif // WAL_OFF
}
void do_scan(DB *db, int id, ColumnFamilyHandle *handle, int *count)
{
#if METHOD == 0
  Iterator *it = db->NewIterator(read_options);
  string prefix = to_string(id);
  if (id == 0)
  {
    it->SeekToFirst();
  }
  else
  {
    it->SeekForPrev(prefix);
    it->Next();
  }
  for (; it->Valid() && it->key().ToString() < to_string(id + 1); it->Next())
  {
    auto s = it->key();
    s = it->value();
    (*count)++;
  }
#else
  Iterator *it = db->NewIterator(read_options, handle);
  for (it->SeekToFirst();
       it->Valid(); it->Next())
  {
    auto k = it->key();
    auto v = it->value();
    (*count)++;
  }
#endif
  delete it;
}
unsigned rand(unsigned *seed)
{
  return *seed = *seed * 1103515245L + 12345L;
}
void *write_worker(void *p)
{
  CTX *ctx = (CTX *)p;
  int id = ctx->id;
  atomic<uint64_t> *write_count = ctx->run_count_ptr;
  struct timeval *start = ctx->start;

  #if KEY_GEN_LOCAL == 1
  uint64_t siz = META_N / THREAD_N;
  char *data = ctx->data_ptr;
  #else
  vector<pair<string, char*>> *m = ctx->v;
  uint64_t siz = m->size();
  #endif
  for (uint64_t i = 0; i < siz; i++)
  {
    #if KEY_GEN_LOCAL == 1
    string _h_key = ctx->LocalGenerateKey({siz, i, THREAD_N, id});
    auto it = pair<std::string, char *>(_h_key, data);
    // int h = id % DB_N;
    #else
    auto &it = m->at(i);
    #endif
    int h = ctx->DBDistribute(it.first);
    int cf = ctx->CFDistribute(it.first);
    DB *db = dbs[h];
    do_write(db, it, id, handles[h][cf]);
    uint64_t c = (*write_count)++;
    if (c % PRINT_INTERVAL_N == 0) {
      struct timeval t2;
      gettimeofday(&t2, NULL);
      long d = (t2.tv_sec - start->tv_sec) * 1000000 + t2.tv_usec - start->tv_usec;
      gettimeofday(start, NULL);
      fprintf(stderr, "WRITE %lu IO: %.3f Mops\n", c, 1.0 * PRINT_INTERVAL_N / d);
    }
  }
  return NULL;
}
void *scan_worker(void *p)
{
  CTX *ctx = (CTX *)p;

  int id = ctx->id;
  int h = (id / CF_N) % DB_N;
  int cf = id % CF_N;
  int count = 0;
  struct timeval *start = ctx->start;

  DB *db = dbs[h];
  do_scan(db, id, handles[h][cf], &count);
  struct timeval t2;
  gettimeofday(&t2, NULL);
  long d = (t2.tv_sec - start->tv_sec) * 1000000 + t2.tv_usec - start->tv_usec;
  fprintf(stderr, "Thread %d scan %d KVs, use time: %.3f ms\n", id, count, d / 1000.0);
  return NULL;
}
void *read_worker(void *p)
{
  CTX *ctx = (CTX *)p;
  int id = ctx->id;
  atomic<uint64_t> *read_count = ctx->run_count_ptr;
  struct timeval *start = ctx->start;

  #if KEY_GEN_LOCAL == 1
  uint64_t siz = META_N / THREAD_N;
  char *data = ctx->data_ptr;
  #else
  vector<pair<string, char*>> *m = ctx->v;
  uint64_t siz = m->size();
  #endif
  for (uint64_t i = 0; i < siz; i++)
  {
    #if KEY_GEN_LOCAL == 1
    string _h_key = ctx->LocalGenerateKey({siz, i, THREAD_N, id});
    auto it = pair<std::string, char *>(_h_key, data);
    // int h = id % DB_N;
    #else
    auto &it = m->at(i);
    #endif
    int h = ctx->DBDistribute(it.first);
    int cf = ctx->CFDistribute(it.first);
    DB *db = dbs[h];
    do_read(db, it, id, handles[h][cf]);
    uint64_t c = (*read_count)++;
    if (c % (PRINT_INTERVAL_N / 2) == 0) {
      struct timeval t2;
      gettimeofday(&t2, NULL);
      long d = (t2.tv_sec - start->tv_sec) * 1000000 + t2.tv_usec - start->tv_usec;
      gettimeofday(start, NULL);
      fprintf(stderr, "READ %lu IO: %.3f Mops\n", c, 1.0 * (PRINT_INTERVAL_N / 2) / d);
    }
  }
  return NULL;
}
void *delete_worker(void *p)
{
  CTX *ctx = (CTX *)p;
  int id = ctx->id;
  atomic<uint64_t> *delete_count = ctx->run_count_ptr;
  struct timeval *start = ctx->start;

  #if KEY_GEN_LOCAL == 1
  uint64_t siz = META_N / THREAD_N;
  char *data = ctx->data_ptr;
  #else
  vector<pair<string, char*>> *m = ctx->v;
  uint64_t siz = m->size();
  #endif
  for (uint64_t i = 0; i < siz; i++)
  {
    #if KEY_GEN_LOCAL == 1
    string _h_key = ctx->LocalGenerateKey({siz, i, THREAD_N, id});
    auto it = pair<std::string, char *>(_h_key, data);
    // int h = id % DB_N;
    #else
    auto &it = m->at(i);
    #endif
    int h = ctx->DBDistribute(it.first);
    int cf = ctx->CFDistribute(it.first);
    DB *db = dbs[h];
    do_delete(db, it, id, handles[h][cf]);
    uint64_t c = (*delete_count)++;
    if (c % PRINT_INTERVAL_N == 0) {
      struct timeval t2;
      gettimeofday(&t2, NULL);
      long d = (t2.tv_sec - start->tv_sec) * 1000000 + t2.tv_usec - start->tv_usec;
      gettimeofday(start, NULL);
      fprintf(stderr, "DELETE %lu IO: %.3f Mops\n", c, 1.0 * PRINT_INTERVAL_N / d);
    }
  }
  return NULL;
}
void getOptions()
{
  // rocksdb options
  options.IncreaseParallelism(32);
  options.OptimizeForPointLookup(0);
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_mmap_reads = true;
  options.use_adaptive_mutex = true;
  options.avoid_unnecessary_blocking_io = true;
  options.unordered_write = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.memtable_whole_key_filtering = true;
  options.compression = kNoCompression;

  options.max_background_jobs = 24;
  options.max_subcompactions = 8;

  // options.compaction_style=kCompactionStyleUniversal会优化写性能，但是会加大读放大
  options.OptimizeLevelStyleCompaction();
  options.write_buffer_size = 64L << 20;
  options.max_write_buffer_number = 24;
  options.min_write_buffer_number_to_merge = 1;
  // 不能level0_file_num_compaction_trigger设为1，否则让flush线程抢占严重
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_readahead_size = 4L << 20;
  options.new_table_reader_for_compaction_inputs = true;
  options.compaction_pri = kOldestSmallestSeqFirst;
  options.level0_slowdown_writes_trigger = 20;
  options.level0_stop_writes_trigger = 10000;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_multiplier = 10;
  options.max_bytes_for_level_base = options.target_file_size_base * options.max_bytes_for_level_multiplier;

  #if WAL_OFF == 1
  write_options.disableWAL = true;
  #else
  write_options.disableWAL = false;
  #endif // WAL_OFF
  read_options.verify_checksums = false;
  read_options.ignore_range_deletions = true;
  read_options.background_purge_on_iterator_cleanup = true;
  table_options.block_size = 4 << 10;
  table_options.no_block_cache = true;
  table_options.checksum = kNoChecksum;
  
  table_options.filter_policy.reset(NewBloomFilterPolicy(12, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.stats_dump_period_sec = 60;
}
void getFileChunkWriteOptions() {
  // rocksdb options
  options.IncreaseParallelism(32);
  options.OptimizeForPointLookup(0);
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.allow_mmap_reads = true;
  options.use_adaptive_mutex = true;
  options.avoid_unnecessary_blocking_io = true;
  options.unordered_write = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.memtable_whole_key_filtering = true;
  options.compression = kNoCompression;

  options.max_background_jobs = 28;
  options.max_subcompactions = 4;

  // options.compaction_style=kCompactionStyleUniversal会优化写性能，但是会加大读放大
  options.OptimizeLevelStyleCompaction();
  options.write_buffer_size = 64L << 20;
  options.max_write_buffer_number = 24;
  options.min_write_buffer_number_to_merge = 1;
  // 不能level0_file_num_compaction_trigger设为1，否则让flush线程抢占严重
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_readahead_size = 4L << 20;
  options.new_table_reader_for_compaction_inputs = true;
  options.compaction_pri = kOldestSmallestSeqFirst;
  options.level0_slowdown_writes_trigger = 20;
  options.level0_stop_writes_trigger = 10000;
  options.target_file_size_base = options.write_buffer_size * 4;
  options.max_bytes_for_level_multiplier = 10;
  options.max_bytes_for_level_base = options.target_file_size_base * options.max_bytes_for_level_multiplier;

  #if WAL_OFF == 1
  write_options.disableWAL = true;
  #else
  write_options.disableWAL = false;
  #endif // WAL_OFF
  read_options.verify_checksums = false;
  read_options.ignore_range_deletions = true;
  read_options.background_purge_on_iterator_cleanup = true;
  table_options.block_size = 16 << 10;
  table_options.no_block_cache = true;
  table_options.checksum = kNoChecksum;
  
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.stats_dump_period_sec = 60;
}
void openDBs() {
  // bind core
	// pthread_spin_init(&rocksdb::flash_ca_lock, 0);
	// for (unsigned i = 1; i <= 30; i++)
	// {
	// 	rocksdb::flash_core_array.push_back(i);
	// }

  getOptions();
  // getFileChunkWriteOptions();

  vector<ColumnFamilyDescriptor> column_families = {ColumnFamilyDescriptor(kDefaultColumnFamilyName, ColumnFamilyOptions(options))};
  #if METHOD == 4
  for (int i = 0; i < CF_N - 1; i++) {
    column_families.push_back(
      ColumnFamilyDescriptor(to_string(i), ColumnFamilyOptions(options))
    );
  }
  #endif // METHOD

  for (int i = 0; i < DB_N; i++) {
    Status s = DB::Open(options, "/mnt/pmem/gyx_xfs_test/kv/tmp" + to_string(i), column_families, handles + i, dbs + i);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.getState());
      assert(s.ok());
    }
  }
#if WAL_OFF == 1
  for (int i = 0; i < THREAD_N; i++) {
    openWal(i);
  }
#endif // WAL_OFF
}
void closeDBs() {
#if METHOD == 4
  for (int j = 0; j < DB_N; j++)
  {
    for (int i = 0; i < handles[j].size(); i++)
    {
      dbs[j]->DestroyColumnFamilyHandle(handles[j][i]);
    }
  } 
#endif // METHOD
  for (int i = 0; i < DB_N; i++)
  {
    delete dbs[i];
  }
#if WAL_OFF == 1
  for (int i = 0; i < THREAD_N; i++) {
    close(fds[i]);
  }
#endif // WAL_OFF
}
void restartDBs() {
  closeDBs();
  openDBs();
}
unsigned seed;

template<typename T>
void shuffle(vector<T> &arr)
{
  for (int i = arr.size() - 1; i >= 0; --i) {
    swap(arr[rand(&seed)%(i+1)], arr[i]);
  }
}
int main()
{
  time_t now;
  now = time(0);
  fprintf(stderr, "===================================\n");
  fprintf(stderr, "=        RocksDB meta test        =\n");
  fprintf(stderr, "===================================\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Test Time: %s\n", ctime(&now));
  fprintf(stderr, "Meta Num: %llu\n", META_N);
  fprintf(stderr, "Key Size: %d B\n", KEY_SIZ);
  fprintf(stderr, "Meta Size: %d B\n", META_SIZ);
  fprintf(stderr, "Chunk Size: %d B\n", CHUNK_SIZ);
  fprintf(stderr, "Thread Num: %d\n", THREAD_N);
  fprintf(stderr, "DB Num: %d\n", DB_N);
  fprintf(stderr, "CF Num: %d\n", CF_N);
  fprintf(stderr, "WAL off: %s\n", (WAL_OFF == 1) ? "true" : "false");
  fprintf(stderr, "KEY GEN locally: %s\n", (KEY_GEN_LOCAL == 1) ? "true" : "false");

  // open db
  openDBs();

  // test data set
  metadata_data = (char*)malloc(META_SIZ);
  chunk_data = (char*)malloc(CHUNK_SIZ);
  memset(metadata_data, 0xaa, META_SIZ);
  memset(chunk_data, 0xaa, CHUNK_SIZ);
  for (int i = 0; i < THREAD_N; i++)
  {
    metadatas[i].clear();
    chunkdatas[i].clear();
  }
  #if KEY_GEN_LOCAL == 0
  for (int i = 0; i < META_N; i++)
  {
    string _k = GenerateKey({i, 0});
    metadatas[keyDistribute(_k)].emplace_back(_k, metadata_data);
  }
  for (int i = META_N; i < META_N * 2; i++)
  {
    string _k = GenerateKey({i, 0});
    chunkdatas[keyDistribute(_k)].emplace_back(_k, chunk_data);
  }
  #endif

  long d;
  struct timeval t1, t2;
  pthread_t thd[THREAD_N];
  struct timeval write_start;
  atomic<uint64_t> write_count = {0};
  struct timeval read_start;
  atomic<uint64_t> read_count = {0};

  // write
  fprintf(stderr, "=========== WRITE META ===========\n");
  for (int i = 0; i < THREAD_N; i++)
  {
    ctxs[i].data_ptr = metadata_data;
    ctxs[i].v = metadatas + i;
    ctxs[i].id = i;
    ctxs[i].run_count_ptr = &write_count;
    ctxs[i].start = &write_start;
    #if KEY_GEN_LOCAL == 1
    ctxs[i].LocalGenerateKey = GenerateHashKey;
    #endif
    ctxs[i].keyDistribute = keyDistribute;
    ctxs[i].DBDistribute = DBDistribute;
    ctxs[i].CFDistribute = CFDistribute;
    shuffle(metadatas[i]);
  }
  write_count = 0;
  gettimeofday(&t1, NULL);
  gettimeofday(&write_start, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, write_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  assert(write_count == META_N);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  fprintf(stderr, "WRITE META IO: %.3f Mops\n", 1.0 * META_N / d);
  
//   // write chunk
//   fprintf(stderr, "=========== WRITE CHUNK ===========\n");
//   for (int i = 0; i < THREAD_N; i++)
//   {
//     ctxs[i].data_ptr = chunk_data;
//     ctxs[i].v = chunkdatas + i;
//     ctxs[i].id = i;
//     ctxs[i].run_count_ptr = &write_count;
//     ctxs[i].start = &write_start;
//     shuffle(chunkdatas[i]);
//   }
//   write_count = 0;
//   gettimeofday(&t1, NULL);
//   gettimeofday(&write_start, NULL);
//   for (int i = 0; i < THREAD_N; i++)
//   {
//     pthread_create(&thd[i], NULL, write_worker, ctxs + i);
//   }
//   for (int i = 0; i < THREAD_N; i++)
//   {
//     pthread_join(thd[i], NULL);
//   }
//   gettimeofday(&t2, NULL);
//   d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//   fprintf(stderr, "WRITE CHUNK IO: %.3f Mops\n", 1.0 * META_N / d);

  // fprintf(stderr, "\n========= Restart DBs ... =========\n\n");

  // random get meta
  fprintf(stderr, "=========== READ META ===========\n");
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].data_ptr = metadata_data;
    ctxs[i].v = metadatas + i;
    ctxs[i].id = i;
    ctxs[i].run_count_ptr = &read_count;
    ctxs[i].start = &read_start;
    shuffle(metadatas[i]);
  }
  read_count = 0;
  gettimeofday(&t1, NULL);
  gettimeofday(&read_start, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, read_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  // assert(read_count == META_N);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  fprintf(stderr, "READ META IO: %.3f Mops\n", 1.0 * META_N / d);

//   // random get chunk
//   fprintf(stderr, "=========== READ CHUNK ===========\n");
//   for (int i = 0; i < THREAD_N; i++) {
//     ctxs[i].data_ptr = chunk_data;
//     ctxs[i].v = chunkdatas + i;
//     ctxs[i].id = i;
//     ctxs[i].run_count_ptr = &read_count;
//     ctxs[i].start = &read_start;
//     shuffle(chunkdatas[i]);
//   }
//   read_count = 0;
//   gettimeofday(&t1, NULL);
//   gettimeofday(&read_start, NULL);
//   for (int i = 0; i < THREAD_N; i++)
//   {
//     pthread_create(&thd[i], NULL, read_worker, ctxs + i);
//   }
//   for (int i = 0; i < THREAD_N; i++)
//   {
//     pthread_join(thd[i], NULL);
//   }
//   gettimeofday(&t2, NULL);
//   d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
//   fprintf(stderr, "READ CHUNK IO: %.3f Mops\n", 1.0 * META_N / d);

  // scan db
  fprintf(stderr, "=========== SCAN DBs ===========\n");
  for (int i = 0; i < DB_N * CF_N; i++) {
    ctxs[i].v = nullptr;
    ctxs[i].id = i;
    ctxs[i].run_count_ptr = &read_count;
    ctxs[i].start = &read_start;
  }
  read_count = 0;
  gettimeofday(&t1, NULL);
  gettimeofday(&read_start, NULL);
  for (int i = 0; i < DB_N * CF_N; i++)
  {
    pthread_create(&thd[i], NULL, scan_worker, ctxs + i);
  }
  for (int i = 0; i < DB_N * CF_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "SCAN ALL TIME: " << 1.0 * d / 1000 << " ms" << endl;

  // random delete meta
  // for (int i = 0; i < THREAD_N; i++) {
  //   ctxs[i].v = metadatas + i;
  //   ctxs[i].id = i;
  //   shuffle(metadatas[i]);
  // }
  // gettimeofday(&t1, NULL);
  // for (int i = 0; i < THREAD_N; i++)
  // {
  //   pthread_create(&thd[i], NULL, delete_worker, ctxs + i);
  // }
  // for (int i = 0; i < THREAD_N; i++)
  // {
  //   pthread_join(thd[i], NULL);
  // }
  // gettimeofday(&t2, NULL);
  // d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  // cout << "DELETE TIME: " << d << " us" << endl;

  // random delete chunk
  // for (int i = 0; i < THREAD_N; i++) {
  //   ctxs[i].v = chunkdatas + i;
  //   ctxs[i].id = i;
  //   shuffle(chunkdatas[i]);
  // }
  // gettimeofday(&t1, NULL);
  // for (int i = 0; i < THREAD_N; i++)
  // {
  //   pthread_create(&thd[i], NULL, delete_worker, ctxs + i);
  // }
  // for (int i = 0; i < THREAD_N; i++)
  // {
  //   pthread_join(thd[i], NULL);
  // }
  // gettimeofday(&t2, NULL);
  // d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  // cout << "DELETE TIME: " << d << " us" << endl;


  // exit
  free(metadata_data);
  free(chunk_data);
  closeDBs();
  now = time(0);
  fprintf(stderr, "\n");
  fprintf(stderr, "Test End: %s\n", ctime(&now));
  return 0;
}