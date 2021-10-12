#include <stdio.h>
#include <unistd.h>
#include <string>
#include <fcntl.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <malloc.h>

using namespace std;

#define KB << 10
#define MB << 20

#define Cap (4 MB)

int main()
{
    const int N = 500;

    char *data = (char*) memalign(4 KB, 8 MB);

    memset(data, 0xaa, Cap);
    struct timeval start, end;
    gettimeofday(&start, NULL);
    for (int i = 0; i < N; i++) {
        int fd = open(("4mb/tmp" + to_string(i)).c_str(),
            O_CREAT | O_RDWR | O_DIRECT);
        assert(write(fd, data, Cap) == (Cap));
        close(fd);
    }
    gettimeofday(&end, NULL);
    long df = (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec);
    printf("avg write 4mb time: %.3f ms\n", df / 1000.0 / N);
    free(data);
    return 0;
}