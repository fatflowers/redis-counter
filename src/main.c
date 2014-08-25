/***
 * redis counter.
 * Parse redis rdb file, count deleted keys, other keys and saved keys.
 * Save key value pair into aof files, format style can be defined with format_kv_handler defined in rediscounter.h and called by rdb_load function.
 *
 * author: sunlei
 * date: 2014.08.22
***/

#include "rediscounter.h"
//#ifndef STRING_H
//#define STRING_H
#include <string.h>
//#endif

/**
 * @brief _format_kv
 * Format a string with key and value, the string is to be dumped in aof file.
 * Define a way to get a aof file number from hashing key.
 * @param key
 * @param key_len
 * @param value
 * @param hashed_key
 * Save hashed aof file number of type int.
 * @return Formatted string of k&v
 */
char * _format_kv(void *key, int key_len, long value, void *hashed_key){
    if(key_len <= 0)
        return NULL;
    *(int *)hashed_key = (((char *)key)[0] - '0') % aof_number;
    char tmp[1024];
    sprintf(tmp, "%s:%ld\n", (char *)key, value);
    return (char *)strdup(tmp);
}

int main(int argc, char **argv)
{
    char *usage = "redis counter usage:\nredis-counter -f rdb file [-n number] [-a aof filename] [-s]\n"
            "\t-f --file \tspecify full path of rdb file would be parsed.\n"
            "\t-n --number \tspecify number of aof files.\n\t\t\tDefault: 1\n"
            "\t-a --name \tspecify name of aof files. \n\t\t\tDefault: output.aof\n"
            "\t-s --save \tSave mode, save aof file. \n\t\t\tDefault: no\n"
            "\t Notice: This tool only test on redis 2.2 and 2.4, so it may be error in 2.4 later.\n";
    if(argc <= 1) {
        fprintf(stderr, "%s", usage);
        exit(1);
    }
    char *rdbFile = NULL;
    int i;
    for(i = 1; i < argc; i++) {
        if(argc > i+1 && argv[i][0] == '-' && argv[i][1] == 'f') {
            i += 1;
            rdbFile = argv[i];
        }else if(argv[i][0] == '-' && argv[i][1] == 'n'){
            i += 1;
            aof_number = atoi(argv[i]);
        }
        else if(argv[i][0] == '-' && argv[i][1] == 'a'){
            i += 1;
            aof_filename = argv[i];
        }
        else if(argv[i][0] == '-' && argv[i][1] == 's'){
            dump_aof = 1;
        }
    }
    rdb_load(rdbFile, _format_kv);
    return 0;
}

