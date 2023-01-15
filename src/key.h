#ifndef _KEY_H
#define _KEY_H

#include <cstring>
#include <string>

#define STRKEY (UINT64_C(0xffffffffffffffff - 1))
#define LOG_KLEN strlen("LOGG") + sizeof(uint64_t) + sizeof(uint16_t);

/*
 * The key object exists so that we're not writing storage and debug
 * logic for the several types of keys across the board.
 */

static void _vernum_to_key(uint64_t vernum, unsigned char *key)
{
    uint32_t klen = sizeof(vernum);
    for(unsigned int i = 0; i < klen; i++) {
        key[i] = vernum >> (8 * (klen - 1 - i)) & 0xFF;
    }
}

class key {
    public:
        key() {
        }

        virtual ~key() = default;

        virtual const char* print() {
            return p_str.c_str();
        }

        virtual void clear() {
            printf("Clearing empty key!\n");
        }

        virtual void* data() {
            return NULL;
        }

        virtual uint16_t len() {
            return UINT16_MAX;
        }

        virtual uint64_t vnum() {
            return UINT64_MAX;
        }
    private:
        std::string p_str = "EMPTY_KEY";
};

class vnum_key : public key {
    public:
        vnum_key(uint64_t _vernum) {
            vernum = _vernum;
            _vernum_to_key(vernum, buf);
            klen = sizeof(vernum);
            sprintf(print_buf, 
                    "COMMIT: %lu SEQ: %lu", vernum >> 32, vernum & 0xFFFFFFFF);
        }

        const char* print() override {
            return print_buf;
        }

        void* data() override {
            return buf;
        }

        uint16_t len() override {
            return klen;
        }

        uint64_t vnum() override {
            return vernum;
        }

        void clear() override {
        }
    private:
        uint16_t klen = sizeof(uint64_t);
        unsigned char buf[16];
        uint64_t vernum = STRKEY;
        char print_buf[128];
};

class log_key : public key {
    public:
        log_key(uint64_t bid, uint16_t _log_num) {
            vernum = bid;
            log_num = _log_num;

            memcpy(buf, "LOGG", strlen("LOGG"));
            memcpy(buf + strlen("LOGG"), &bid, sizeof(bid));
            memcpy(buf + strlen("LOGG") + sizeof(bid), &log_num, sizeof(log_num));

            sprintf(print_buf, "LOG. BID: %lu LOG_NUM: %u", vernum, log_num);
        }

        const char* print() override {
            return print_buf;
        }       

        void* data() override {
            return buf;
        }

        uint16_t len() override {
            return klen;
        }

        uint64_t vnum() override {
            return vernum;
        }

        void clear() override {
            memset(buf, 0x0, sizeof(buf));
            vernum = STRKEY;
            log_num = UINT16_MAX;
            memset(print_buf, 0x0, sizeof(print_buf));
        }

    private:
        uint16_t klen = strlen("LOGG") + sizeof(uint64_t) + sizeof(uint16_t);
        unsigned char buf[16];
        uint64_t vernum = STRKEY;
        uint16_t log_num;
        char print_buf[128];
};

class str_key : public key {
    public:
        str_key(std::string _str) {
            str = _str;
            klen = str.length();
        }

        const char* print() override {
            return str.c_str();
        }

        void* data() override {
            return (void*)str.c_str();
        }

        uint16_t len() override {
            return klen;
        }

        void clear() override {
        }
    private:
        std::string str;
        uint64_t vernum = BLK_NOT_FOUND;
        uint16_t klen;
};

#endif
