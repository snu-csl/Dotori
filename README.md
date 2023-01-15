## Compiling

```bash
# Using a real KVSSD
build.sh REAL

# Using the emulator
# Works, but performance won't be representative.
build.sh EMU

# Remove the KVSSD, Dotori, and benchmark build folders
build.sh CLEAN
```

This will build the KVSSD software, the Dotori library, and the benchmark.
See inside the script for each step. The Dotori library and the KVSSD libraries
will be copied to the resulting build folder.

## Running the benchmarks

### Preconditioning

To precondition the KVSSD, update precondition.sh to point to your KVSSD path (e.g. /dev/nvme0n1) and run it.
The process will take several hours.
The preconditioning stage is not required to run the YCSB benchmarks below.

### YCSB

Included in the benchmark directory is the set of configuration files used for the YCSB tests. To run a test :

```bash
cd bench/dotori
nvme format /dev/nvme0n1

# Populate the database and run the benchmark
# Requires sudo if using a real KVSSD
LD_LIBRARY_PATH=../../build/ ./dotori_bench -f ../ycsb_med/ycsba_uniform.ini

# Skip population and run the benchmark only
# Won't work on the emulator, as closing the emulator (in the previous population run) wipes the data
# Requires sudo if using a real KVSSD
LD_LIBRARY_PATH=../../build/ ./dotori_bench -e -f ../ycsb_med/ycsba_uniform.ini
```

### Large dataset performance test

```bash
cd bench/dotori

# Populate the database and run the benchmark
# Requires sudo if using a real KVSSD
# The test where roughly 55% of the device is filled
LD_LIBRARY_PATH=../../build/ ./dotori_bench -f ../large_dataset_test/big_55.ini

# The test where roughly 80% of the device is filled
LD_LIBRARY_PATH=../../build/ ./dotori_bench -f ../large_dataset_test/big_80.ini
```

## Comments 

Dotori started as an extension of ForestDB, and thus the code still contains a lot of block-device and file-related references and naming schemes. Working with blocks and files isn't really required when using a KVSSD. This also means that there are a lot of references to "fdb" instead of "dotori" in the code.

For examples of how to use Dotori, please see [the benchmark wrapper](bench/wrappers/couch\_dotori.cc) and [the functionality tests](tests/functional/fdb\_functional\_test.cc).

## Acknowledgements

We thank the authors of https://github.com/BLepers/KVell and https://github.com/cameron314/concurrentqueue, whose work we modified and used in Dotori.
