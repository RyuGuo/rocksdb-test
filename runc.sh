g++ -g c.cpp -pthread -lwiredtiger -o c.out
rm -rf ./wtkv/*
sudo echo 3 > /proc/sys/vm/drop_caches
LD_LIBRARY_PATH=/usr/local/lib ./c.out