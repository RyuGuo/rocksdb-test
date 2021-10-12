/**
 * 测试wiredtiger并发写入
 * * 多线程写入
 * * 多实例写入
 */

#include <iostream>
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

#include "wt.h"

using namespace std;
using namespace WT;

#define META_N 10000000
#define THREAD_N 1
#define DB_N 1

#define KEY_SIZ 80
//metadata size
#define META_SIZ 48
//chunk size
#define CHUNK_SIZ 4096

/**
 * 0 - 单实例多线程写入
 * 1 - 多实例写入
 * 2 - 单实例单线程读出
 * 3 - 多实例写入，自己实现WAL
 * 4 - 单实例多table写入，自己实现WAL
 */
#define METHOD 1

struct CTX {
  vector<pair<string, char*>> *v;
  int id;
};

CTX ctxs[THREAD_N];
vector<pair<string, char*>> metadatas[THREAD_N];
vector<pair<string, char*>> chunkdatas[THREAD_N];
DB *dbs[DB_N];
DBSession *sessions[DB_N][THREAD_N];
Options options, read_options, write_options;

int fds[THREAD_N];
char *metadata_data;
char *chunk_data;

void openWal(int id)
{
  fds[id] = open(("kv/wal" + to_string(id)).c_str(), O_CREAT | O_RDWR);
}
void wal(int id, pair<string, char*> &p)
{
  string s = p.first + p.second;
  // assert(write(fds[id], s.c_str(), s.size()) > 0);
}

void do_write(DBSession *db, pair<string, char*> &p, int id)
{
#if METHOD == 0
  db->Put(write_options, to_string(id % DB_N) + p.first, p.second);
#elif METHOD == 1 || METHOD == 2
  db->Put(write_options, p.first, p.second);
#elif METHOD == 3
  db->Put(write_options, p.first, p.second);
  wal(id, p);
#elif METHOD == 4
  db->Put(write_options, p.first, p.second);
  wal(id, p);
#endif
}
void do_read(DBSession *db, pair<string, char*> &p, int id)
{
  string res;
#if METHOD == 0
  db->Get(read_options, to_string(id % DB_N) + p.first, &res);
#elif METHOD == 1 || METHOD == 2 || METHOD == 3
  db->Get(read_options, p.first, &res);
#elif METHOD == 4
  db->Get(read_options, p.first, &res);
#endif
}
void do_scan(DBSession *db, int id)
{
  int count = 0;
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
    count++;
  }
#elif METHOD == 1 || METHOD == 2 || METHOD == 3
  Iterator *it = db->NewIterator(read_options);
  for (it->SeekToFirst();
       it->Valid(); it->Next())
  {
    auto s = it->key();
    s = it->value();
    count++;
  }
#elif METHOD == 4
  Iterator *it = db->NewIterator(read_options);
  for (it->SeekToFirst();
       it->Valid(); it->Next())
  {
    auto s = it->key();
    s = it->value();
    count++;
  }
#endif
  printf("Thread %d scan %d KVs.\n", id, count);
  delete it;
}
void do_delete(DBSession *db, pair<string, char*> &p, int id)
{
#if METHOD == 0
  db->Delete(write_options, to_string(id % DB_N) + p.first);
#elif METHOD == 1 || METHOD == 2
  db->Delete(write_options, p.first);
#elif METHOD == 3
  db->Delete(write_options, p.first);
  wal(id, p);
#elif METHOD == 4
  db->Delete(write_options, handle, p.first);
  wal(id, p);
#endif
}
unsigned rand(unsigned *seed)
{
  return *seed = *seed * 1103515245L + 12345L;
}
void *write_worker(void *p)
{
  CTX *ctx = (CTX *)p;

  vector<pair<string, char*>> *m = ctx->v;
  int id = ctx->id;
  int siz = m->size();

  for (int i = 0; i < siz; i++)
  {
    auto &it = m->at(i);
    int h = hash<string>{}(it.first) % DB_N;
    // wt
    #if METHOD == 0 || METHOD == 2
      DB *db = dbs[0];
      DBSession *session = sessions[0][id];
    #elif METHOD == 1 || METHOD == 3 || METHOD == 4
      DB *db = dbs[h];
      DBSession *session = sessions[h][id];
    #endif
    do_write(session, it, id);
  }
  return NULL;
}
void *scan_worker(void *p)
{
  CTX *ctx = (CTX *)p;

  int id = ctx->id;

// wt
#if METHOD == 0 || METHOD == 2
  DB *db = dbs[0];
  DBSession *session = sessions[0][id];
#elif METHOD == 1 || METHOD == 3 || METHOD == 4
  DB *db = dbs[id];
  DBSession *session = sessions[id][id];
#endif
  do_scan(session, id);
  return NULL;
}
void *read_worker(void *p)
{
  CTX *ctx = (CTX *)p;

  vector<pair<string, char*>> *m = ctx->v;
  int id = ctx->id;
  int siz = m->size();

  for (int i = 0; i < siz; i++)
  {
    auto &it = m->at(i);
    int h = hash<string>{}(it.first) % DB_N;
    // wt
    #if METHOD == 0 || METHOD == 2
      DB *db = dbs[0];
      DBSession *session = sessions[0][id];
    #elif METHOD == 1 || METHOD == 3 || METHOD == 4
      DB *db = dbs[h];
      DBSession *session = sessions[h][id];
    #endif
    do_read(session, it, id);
  }
  return NULL;
}
void *delete_worker(void *p)
{
  CTX *ctx = (CTX *)p;

  vector<pair<string, char*>> *m = ctx->v;
  int id = ctx->id;
  int siz = m->size();

  for (int i = 0; i < siz; i++)
  {
    auto &it = m->at(i);
    int h = hash<string>{}(it.first) % DB_N;
    // rocksdb
    #if METHOD == 0 || METHOD == 2 || METHOD == 4
      DB *db = dbs[0];
      DBSession *session = sessions[0][id];
    #elif METHOD == 1 || METHOD == 3
      DB *db = dbs[h];
      DBSession *session = sessions[h][id];
    #endif
    do_delete(session, it, id);
  }
  return NULL;
}
void getOptions()
{
  options.options = (char*)"create,log=(enabled=true),cache_size=8GB,direct_io=[data],session_max=500";
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
  long d;

  // open db
  getOptions();

#if METHOD == 0 || METHOD == 2
  mkdir(("wtkv/tmp" + to_string(0)).c_str(), 0775);
  Status s = DB::Open(options, "wtkv/tmp" + to_string(0), dbs + 0);
  assert(s.ok());
  for (int j = 0; j < THREAD_N; j++) {
    sessions[0][j] = new DBSession(dbs[0]);
  }
#elif METHOD == 1 || METHOD == 3
  for (int i = 0; i < DB_N; i++) {
    mkdir(("wtkv/tmp" + to_string(i)).c_str(), 0775);
    Status s = DB::Open(options, "wtkv/tmp" + to_string(i), dbs + i);
    assert(s.ok());
    for (int j = 0; j < THREAD_N; j++) {
      sessions[i][j] = new DBSession(dbs[i]);
    }
  }
#elif METHOD == 4
  mkdir(("wtkv/tmp" + to_string(0)).c_str(), 0775);
  Status s = DB::Open(options, "wtkv/tmp" + to_string(0), dbs + 0);
  assert(s.ok());
  for (int i = 0; i < DB_N; i++) {
    for (int j = 0; j < THREAD_N; j++) {
      sessions[i][j] = new DBSession(dbs[0], "m" + to_string(i));
    }
  }
#endif
  for (int i = 0; i < THREAD_N; i++) {
    openWal(i);
  }

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
  for (int i = 0; i < META_N; i++)
  {
    char key[KEY_SIZ];
    sprintf(key, "%063d", i);
    metadatas[hash<string>{}(to_string(i)) % THREAD_N].push_back(make_pair(string(key), metadata_data));
  }
  for (int i = META_N; i < META_N * 2; i++)
  {
    char key[KEY_SIZ];
    sprintf(key, "%063d", i);
    chunkdatas[hash<string>{}(to_string(i)) % THREAD_N].push_back(make_pair(string(key), chunk_data));
  }

  struct timeval t1, t2;
  pthread_t thd[THREAD_N];

  // write
  for (int i = 0; i < THREAD_N; i++)
  {
    ctxs[i].v = metadatas + i;
    ctxs[i].id = i;
    shuffle(metadatas[i]);
  }
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, write_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "WRITE TIME: " << d << " us" << endl;
  
  for (int i = 0; i < THREAD_N; i++)
  {
    ctxs[i].v = chunkdatas + i;
    ctxs[i].id = i;
    shuffle(chunkdatas[i]);
  }
  // write chunk
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, write_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "WRITE TIME: " << d << " us" << endl;

  // random get meta
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].v = metadatas + i;
    ctxs[i].id = i;
    shuffle(metadatas[i]);
  }
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, read_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "READ TIME: " << d << " us" << endl;

  // random get chunk
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].v = chunkdatas + i;
    ctxs[i].id = i;
    shuffle(chunkdatas[i]);
  }
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, read_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "READ TIME: " << d << " us" << endl;

  // scan db
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].v = nullptr;
    ctxs[i].id = i;
  }
  gettimeofday(&t1, NULL);
  #if METHOD == 0 || METHOD == 1 || METHOD == 3 || METHOD == 4
  for (int i = 0; i < DB_N; i++)
  {
    pthread_create(&thd[i], NULL, scan_worker, ctxs + i);
  }
  for (int i = 0; i < DB_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  #elif METHOD == 2
  DB *db = dbs[0];
  do_scan(db, 0);
  #endif
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "SCAN TIME: " << d << " us" << endl;

  // random delete meta
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].v = metadatas + i;
    ctxs[i].id = i;
    shuffle(metadatas[i]);
  }
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, delete_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "DELETE TIME: " << d << " us" << endl;

  // random delete chunk
  for (int i = 0; i < THREAD_N; i++) {
    ctxs[i].v = chunkdatas + i;
    ctxs[i].id = i;
    shuffle(chunkdatas[i]);
  }
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, delete_worker, ctxs + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "DELETE TIME: " << d << " us" << endl;

  // exit
#if METHOD == 0 || METHOD == 2
  for (int j = 0; j < THREAD_N; j++) {
    delete sessions[0][j];
  }
  delete dbs[0];
#elif METHOD == 1 || METHOD == 3
  for (int i = 0; i < DB_N; i++)
  {
    for (int j = 0; j < THREAD_N; j++) {
      delete sessions[i][j];
    }
    delete dbs[i];
  }
#elif METHOD == 4
  delete dbs[0];
#endif
  for (int i = 0; i < THREAD_N; i++) {
    close(fds[i]);
  }

  return 0;
}