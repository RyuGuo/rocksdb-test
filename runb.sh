g++ -g b.cpp -pthread -lrocksdb -o b.out
rm -rf kv
mkdir kv
sudo echo 3 > /proc/sys/vm/drop_caches
LD_LIBRARY_PATH=/usr/local/lib ./b.out >> b.txt 2>&1