#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage : ./build.sh REAL/EMU/CLEAN"
    exit
fi

START=$(pwd)
TYPE=$1
if [ "$TYPE" != "REAL" ] && [ "$TYPE" != "EMU" ] && [ "$TYPE" != "CLEAN" ]; then
    echo "Invalid build type (use REAL, EMU, or CLEAN)"
    exit
fi

if [ "$TYPE" == "CLEAN" ]; then
    rm -rf KVSSD/PDK/core/build
    rm -rf bench/dotori
    rm -rf build
    exit
fi

# Build the KVSSD software
cd KVSSD/PDK/core/
mkdir build
cd build
if [ "$TYPE" == "REAL" ]; then
    cmake -DWITH_KDD=ON ../
else
    cmake -DWITH_EMU=ON ../
fi
cmake -DCMAKE_BUILD_TYPE=Release ../
make -j8

# Build Dotori
cd $START
mkdir build
cd build

if [ "$TYPE" == "REAL" ]; then
    cmake -DKVSSD_EMU=OFF ../
else
    cmake -DKVSSD_EMU=ON ../
fi
cmake -DCMAKE_BUILD_TYPE=Release ../
make -j8 dotori
cp $START/KVSSD/PDK/core/build/libkvapi.so .

# Build the benchmark
cd $START
cd bench
mkdir dotori
cd dotori
cmake ../
cmake -DCMAKE_BUILD_TYPE=Release ../
LD_LIBRARY_PATH=$START/build make -j8 dotori_bench

cd $START
