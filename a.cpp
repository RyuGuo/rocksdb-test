/**
 * 测试FS写入性能（）
 * * 单目录写入
 * * 多目录并发写入
 */


#include <iostream>
#include <sys/time.h>
#include <pthread.h>
#include <vector>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <malloc.h>

using namespace std;

// 文件数量
#define FILE_N 100
#define THREAD_N 4
#define TEST_CNT 1250
// 4MB size
#define BLK_SIZ (4L << 20)

/**
 * 0 - 常规
 * 1 - 线程独占目录
 */
#define METHOD 1

vector<pair<string, int>> files[THREAD_N];
unsigned char *data[THREAD_N];

void do_write(const char *file_name, int blk_id, int siz, unsigned char *data, int id)
{
  char fn[40];
  #if METHOD == 0
  sprintf(fn, "io/%s-%d", file_name, blk_id);
  #elif METHOD == 1
  char dn[40];
  sprintf(dn, "io/%d", blk_id);
  sprintf(fn, "io/%d/%s-%d", id, file_name, blk_id);
  #endif 
  int fd = open(fn, O_CREAT | O_RDWR | O_DIRECT);
  write(fd, data, siz);
  close(fd);
}

unsigned rand(unsigned *seed)
{
  return *seed = *seed * 1103515245L + 12345L;
}
void *worker(void *p)
{
  vector<pair<string, int>> *f = (vector<pair<string, int>> *)p;
  int id = f - files;
  int siz = f->size();
  unsigned seed = (unsigned)(0xffff & (unsigned long)p);
  #if METHOD == 1
  char dn[40];
  sprintf(dn, "io/%d", id);
  mkdir(dn, 0775);
  #endif
  for (int i = 0; i < TEST_CNT; i++)
  {
    // 随机选一个文件，递增chunk写入
    auto &it = (*f)[rand(&seed) % siz];
    do_write(it.first.c_str(), ++it.second, BLK_SIZ, data[id], id);
  }
  return NULL;
}
int main()
{
  for (int i = 0; i < FILE_N; i++)
  {
    files[i % THREAD_N].push_back(make_pair(to_string(i), 0));
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    data[i] = (unsigned char *)memalign(4 << 10, BLK_SIZ);
    memset(data[i], 0xaa, BLK_SIZ);
  }
  struct timeval t1, t2;
  pthread_t thd[THREAD_N];
  gettimeofday(&t1, NULL);
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_create(&thd[i], NULL, worker, files + i);
  }
  for (int i = 0; i < THREAD_N; i++)
  {
    pthread_join(thd[i], NULL);
  }
  gettimeofday(&t2, NULL);
  long d = (t2.tv_sec - t1.tv_sec) * 1000000 + t2.tv_usec - t1.tv_usec;
  cout << "TIME: " << (d / 5000.0) << " us" << endl;
}