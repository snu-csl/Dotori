#include "filemgr_ops_kvssd.h"

struct filemgr_ops_kvssd * get_samsung_kvssd_filemgr_ops();

struct filemgr_ops_kvssd * get_filemgr_ops_kvssd()
{
#if defined(WIN32) || defined(_WIN32)
    // windows
    return NULL;
#else
    // linux, mac os x
    return get_samsung_kvssd_filemgr_ops();
#endif
}
