Technique Dependence:
1. run on little endian machine (All machines with Intel CPU are little endian)
2. compiler with part of C++17 support

Install GCC 5 before compiling source code (Ubuntu 14.04 LTS X64):
1. sudo add-apt-repository ppa:ubuntu-toolchain-r/test
2. sudo apt-get update
3. sudo apt-get install gcc-5 g++-5
4. sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-5 60 --slave /usr/bin/g++ g++ /usr/bin/g++-5

Compile Steps by CMake (https://cmake.org/):
1. create a build folder outside the src folder
   (eg: mkdir build_minsizerel)
2. change directory to the build folder
   (eg: cd build_minsizerel)
3. cmake 'path_to_source_root' -DCMAKE_BUILD_TYPE=MINSIZEREL
   (eg: cmake ../src -DCMAKE_BUILD_TYPE=MINSIZEREL)
4. make

