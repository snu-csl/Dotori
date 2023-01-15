#add_executable(hash_test
#               ${ROOT_SRC}/avltree.cc
#               ${ROOT_SRC}/hash.cc
#               ${ROOT_SRC}/hash_functions.cc
#               ${ROOT_SRC}/list.cc
#               ${GETTIMEOFDAY_VS}
#               hash_test.cc
#               ${ROOT_UTILS}/memleak.cc)
#target_link_libraries(hash_test ${PTHREAD_LIB} ${LIBM} ${MALLOC_LIBRARIES})
#
#add_executable(bcache_test
#               bcache_test.cc
#               ${ROOT_SRC}/avltree.cc
#               ${ROOT_SRC}/bgflusher.cc
#               ${ROOT_SRC}/blockcache.cc
#               ${ROOT_SRC}/btree.cc
#               ${PROJECT_SOURCE_DIR}/${BREAKPAD_SRC}
#               ${ROOT_SRC}/checksum.cc
#               ${ROOT_SRC}/encryption.cc
#               ${ROOT_SRC}/encryption_aes.cc
#               ${ROOT_SRC}/encryption_bogus.cc
#               ${ROOT_SRC}/filemgr.cc
#               ${ROOT_SRC}/filemgr_ops.cc
#               ${ROOT_SRC}/filemgr_ops_kvssd.cc
#               ${ROOT_SRC}/forestdb.cc
#               ${PROJECT_SOURCE_DIR}/${FORESTDB_FILE_OPS}
#               ${GETTIMEOFDAY_VS}
#               ${KVSSD_IO_SRC}
#               ${ROOT_SRC}/profiling.cc
#               ${ROOT_SRC}/kv_data_cache.cc
#               ${ROOT_SRC}/hash.cc
#               ${ROOT_SRC}/hash_functions.cc
#               ${ROOT_SRC}/list.cc
#               ${ROOT_SRC}/log_message.cc
#               ${ROOT_SRC}/wal.cc
#               ${ROOT_SRC}/version.cc
#               ${ROOT_UTILS}/crc32.cc
#               ${ROOT_UTILS}/debug.cc
#               ${ROOT_UTILS}/memleak.cc
#               ${ROOT_UTILS}/partiallock.cc
#               ${ROOT_UTILS}/time_utils.cc)
#target_link_libraries(bcache_test ${PTHREAD_LIB} ${LIBM}
#                      ${ASYNC_IO_LIB} ${MALLOC_LIBRARIES}
#                      ${PLATFORM_LIBRARY} ${CRYPTO_LIB}
#                      ${DL_LIBRARIES} ${BREAKPAD_LIBRARIES} ${KVAPI_LIBRARIES})
#set_target_properties(bcache_test PROPERTIES COMPILE_FLAGS "${CB_GNU_CXX11_OPTION}")
#
#add_executable(atomic_test
#               atomic_test.cc
#               ${GETTIMEOFDAY_VS})
#target_link_libraries(atomic_test ${PTHREAD_LIB} ${LIBM})
#
#add_executable(filemgr_test
#               filemgr_test.cc
#               ${ROOT_SRC}/avltree.cc
#               ${ROOT_SRC}/bgflusher.cc
#               ${ROOT_SRC}/blockcache.cc
#               ${ROOT_SRC}/btree.cc
#               ${PROJECT_SOURCE_DIR}/${BREAKPAD_SRC}
#               ${ROOT_SRC}/checksum.cc
#               ${ROOT_SRC}/encryption.cc
#               ${ROOT_SRC}/encryption_aes.cc
#               ${ROOT_SRC}/encryption_bogus.cc
#               ${ROOT_SRC}/filemgr.cc
#               ${ROOT_SRC}/filemgr_ops.cc
#               ${ROOT_SRC}/filemgr_ops_kvssd.cc
#               ${ROOT_SRC}/forestdb.cc
#               ${PROJECT_SOURCE_DIR}/${FORESTDB_FILE_OPS}
#               ${GETTIMEOFDAY_VS}
#               ${KVSSD_IO_SRC}
#               ${ROOT_SRC}/profiling.cc
#               ${ROOT_SRC}/kv_data_cache.cc
#               ${ROOT_SRC}/hash.cc
#               ${ROOT_SRC}/hash_functions.cc
#               ${ROOT_SRC}/list.cc
#               ${ROOT_SRC}/log_message.cc
#               ${ROOT_SRC}/wal.cc
#               ${ROOT_SRC}/version.cc
#               ${ROOT_UTILS}/crc32.cc
#               ${ROOT_UTILS}/debug.cc
#               ${ROOT_UTILS}/memleak.cc
#               ${ROOT_UTILS}/partiallock.cc
#               ${ROOT_UTILS}/time_utils.cc)
#target_link_libraries(filemgr_test ${PTHREAD_LIB} ${LIBM}
#                      ${ASYNC_IO_LIB} ${MALLOC_LIBRARIES}
#                      ${PLATFORM_LIBRARY} ${CRYPTO_LIB}
#                      ${DL_LIBRARIES} ${BREAKPAD_LIBRARIES} ${KVAPI_LIBRARIES})
#set_target_properties(filemgr_test PROPERTIES COMPILE_FLAGS "${CB_GNU_CXX11_OPTION}")
#
#add_executable(btreeblock_test_kvssd
#        ${PROJECT_SOURCE_DIR}/${FORESTDB_FILE_OPS}
#        btreeblock_test_kvssd.cc
#        ${GETTIMEOFDAY_VS}
#        $<TARGET_OBJECTS:FDB_TOOLS_CORE>
#        $<TARGET_OBJECTS:FDB_TOOLS_UTILS>)
#target_link_libraries(btreeblock_test_kvssd ${PTHREAD_LIB} ${LIBM}
#        ${SNAPPY_LIBRARIES} ${ASYNC_IO_LIB}
#        ${MALLOC_LIBRARIES} ${PLATFORM_LIBRARY}
#        ${LIBRT} ${CRYPTO_LIB}
#        ${DL_LIBRARIES} ${BREAKPAD_LIBRARIES} ${KVAPI_LIBRARIES})
#set_target_properties(btreeblock_test_kvssd PROPERTIES COMPILE_FLAGS "-D_FDB_TOOLS")
#
#add_executable(docio_test
#               docio_test.cc
#               ${ROOT_SRC}/avltree.cc
#               ${ROOT_SRC}/bgflusher.cc
#               ${ROOT_SRC}/blockcache.cc
#               ${ROOT_SRC}/btree.cc
#               ${PROJECT_SOURCE_DIR}/${BREAKPAD_SRC}
#               ${ROOT_SRC}/checksum.cc
#               ${ROOT_SRC}/docio.cc
#               ${ROOT_SRC}/encryption.cc
#               ${ROOT_SRC}/encryption_aes.cc
#               ${ROOT_SRC}/encryption_bogus.cc
#               ${ROOT_SRC}/filemgr.cc
#               ${ROOT_SRC}/filemgr_ops.cc
#               ${ROOT_SRC}/filemgr_ops_kvssd.cc
#               ${ROOT_SRC}/forestdb.cc
#               ${PROJECT_SOURCE_DIR}/${FORESTDB_FILE_OPS}
#               ${GETTIMEOFDAY_VS}
#               ${KVSSD_IO_SRC}
#               ${ROOT_SRC}/profiling.cc
#               ${ROOT_SRC}/kv_data_cache.cc
#               ${ROOT_SRC}/hash.cc
#               ${ROOT_SRC}/hash_functions.cc
#               ${ROOT_SRC}/list.cc
#               ${ROOT_SRC}/log_message.cc
#               ${ROOT_SRC}/wal.cc
#               ${ROOT_SRC}/version.cc
#               ${ROOT_UTILS}/crc32.cc
#               ${ROOT_UTILS}/debug.cc
#               ${ROOT_UTILS}/memleak.cc
#               ${ROOT_UTILS}/partiallock.cc
#               ${ROOT_UTILS}/time_utils.cc)
#target_link_libraries(docio_test ${PTHREAD_LIB} ${LIBM} ${SNAPPY_LIBRARIES}
#                      ${ASYNC_IO_LIB} ${MALLOC_LIBRARIES}
#                      ${PLATFORM_LIBRARY} ${CRYPTO_LIB}
#                      ${DL_LIBRARIES} ${BREAKPAD_LIBRARIES} ${KVAPI_LIBRARIES})
#set_target_properties(docio_test PROPERTIES COMPILE_FLAGS "${CB_GNU_CXX11_OPTION}")
#
#add_executable(btree_str_kv_test
#               btree_str_kv_test.cc
#               ${ROOT_SRC}/btree_str_kv.cc
#               ${ROOT_SRC}/avltree.cc
#               ${GETTIMEOFDAY_VS}
#               ${ROOT_UTILS}/memleak.cc)
#target_link_libraries(btree_str_kv_test  ${LIBM} ${MALLOC_LIBRARIES})
#
#add_executable(btree_kv_test
#               btree_kv_test.cc
#               ${ROOT_SRC}/btree_kv.cc
#               ${ROOT_SRC}/avltree.cc
#               ${GETTIMEOFDAY_VS}
#               ${ROOT_UTILS}/memleak.cc)
#target_link_libraries(btree_kv_test ${LIBM} ${MALLOC_LIBRARIES})
#
#  add test target
#add_test(hash_test hash_test)
#add_test(bcache_test bcache_test)
#add_test(atomic_test atomic_test)
#add_test(filemgr_test filemgr_test)
#add_test(btreeblock_test btreeblock_test)
#add_test(docio_test docio_test)
#add_test(btree_str_kv_test btree_str_kv_test)
#add_test(btree_kv_test btree_kv_test)
#ADD_CUSTOM_TARGET(unit_tests
#    COMMAND ctest
#)
