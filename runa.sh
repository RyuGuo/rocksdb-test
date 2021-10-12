g++ a.cpp -pthread
rm -rf ./io/*
sudo echo 3 > /proc/sys/vm/drop_caches
./a.out
