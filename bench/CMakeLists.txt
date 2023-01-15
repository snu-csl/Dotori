cmake_minimum_required(VERSION 2.8)

include_directories(BEFORE ${CMAKE_SOURCE_DIR}/utils)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/bench)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/include)
include_directories(BEFORE ${CMAKE_SOURCE_DIR}/wrappers)
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/../include)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_GNU_SOURCE -D__DEBUG -g")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif(NOT CMAKE_BUILD_TYPE)

if (NOT WIN32)
    set(PTHREAD_LIB pthread)
    set(LIBM m)
    set(LIBZ z)
    set(LIBBZ2 bz2)
    set(LIBDOTORI ${CMAKE_CURRENT_SOURCE_DIR}/../build/libdotori.so)
    set(LIBKVAPI ${CMAKE_CURRENT_SOURCE_DIR}/../build/libkvapi.so)
endif(NOT WIN32)

if ("${CMAKE_C_COMPILER_ID}" STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments -g -fomit-frame-pointer -pthread")
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fomit-frame-pointer -pthread")
    set(LIBRT rt)
endif()

add_executable(dotori_bench
               bench/couch_bench.cc
               wrappers/couch_dotori.cc
               utils/avltree.cc
               utils/stopwatch.cc
               utils/iniparser.cc
               utils/crc32.cc
               utils/memleak.cc
               utils/zipfian_random.cc
               utils/keyloader.cc
               utils/keygen.cc
               utils/latest-generator.cc
               utils/zipf.cc)
target_link_libraries(dotori_bench ${PTHREAD_LIB} ${LIBDOTORI})
set_target_properties(dotori_bench PROPERTIES COMPILE_FLAGS "-D__DOTORI_BENCH")
file(COPY ${CMAKE_SOURCE_DIR}/bench_config.ini DESTINATION ./)
