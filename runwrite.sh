g++ write4mb.cpp -o write4mb
rm -rf ./4mb/*
sudo echo 3 > /proc/sys/vm/drop_caches
./write4mb
