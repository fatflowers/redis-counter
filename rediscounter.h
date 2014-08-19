#ifndef REDISCOUNTER_H
#define REDISCOUNTER_H

//defination of key-value formatting
typedef char * format_kv(void * key, int key_len, long value);
typedef struct rdb_state{
    long long offset;
    long long size;
    long long used;
    long long deleted;
    long long key_size;
    long long entry_size;
    long long value_size;
    char * rdb_filename;
}rdb_state;

#endif // REDISCOUNTER_H
