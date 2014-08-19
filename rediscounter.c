#include "rediscounter.h"

/* Aof class sunlei*/
long long REDISCOUNTER_RDB_BLOCK = 10240;
typedef struct Aof{
    int index;
    char *filename;
    sds buffer;
}Aof;

void init_aof(Aof * aof_obj, int index, char *filename){
    aof_obj->index = index;
    if((aof_obj->filename = (char *)malloc(sizeof(char) * (strlen(filename) + 1))) == NULL)
        goto err;
    strcpy(aof_obj->filename, filename);
    if((aof_obj->buffer = sdsnewlen(NULL, AOF_BUFFER_SIZE)) == NULL)
        goto err;

err:
    free(aof_obj->filename);
    sdsfree(aof_obj->buffer);
}

//time_used function to be added
int save_aof(Aof * aof_obj){
    if(aof_obj->buffer)
        return 0;
    FILE *fp;
    if((fp = fopen(aof_obj->filename, "ab+")) == NULL)
        goto err;
    if(fwrite(aof_obj->buffer, strlen(aof_obj->buffer), 1, fp) != 1)
        goto err;
    return 0;
err:
    fclose(fp);
    return -1;
}

int add_aof(Aof * aof_obj, char * item){
    if(aof_obj->buffer == NULL || item == NULL)
        return -1;
    sdscat(aof_obj->buffer, item);
    if(sdslen(aof_obj->buffer) > AOF_BUFFER_SIZE)
        save_aof(aof_obj);

    return 0;
}

unsigned char read_byte(FILE * fp){
    unsigned char number;
    if (fread_check(&number, 1, 1, fp) == 0) return -1;
    return number;
}

long long rdb_get_long(FILE * fp){
    long long number;
    unsigned char digits = read_byte(fp);
    if(digits > RDB_INVALID_LEN)
        return -1;
    if (fread_check(&number, digits, 1, fp) == 0) return -1;
    return number;
}

//default format kv, need to free result, is it ok?
char * _format_kv(char *key, int value){
    char *result = (char *)malloc(sizeof(char) * (strlen(key) + strlen(value) + 2));
    sprintf(result, "%s:%d", key, value);
    return result;
}

int _key_hash(char * key, int value){
    return 0;
}

/* rdb parse */
rdb_state state;

void adjust_block_size(long long entry_size){
    printf(("REDISCOUNTER_RDB_BLOCK=%d", REDISCOUNTER_RDB_BLOCK));
    REDISCOUNTER_RDB_BLOCK = entry_size * ((REDISCOUNTER_RDB_BLOCK + entry_size - 1) / entry_size);
    printf(("REDISCOUNTER_RDB_BLOCK=%d", REDISCOUNTER_RDB_BLOCK));
}

void rdb_get_dict(){
    adjust_block_size(state.entry_size);
    long long total = state.size * state.entry_size,
            count = total / REDISCOUNTER_RDB_BLOCK,
            rest = total % REDISCOUNTER_RDB_BLOCK,
            key_count,
            nkey_read;
    sds empty_key = sdsnewlen(NULL, state.key_size),
            deleted_key = sdsnewlen(NULL, state.key_size),
            buf;
    int i, j, value;
    char key
    for(i = 0; i < state.key_size; i++){
        empty_key[i] = '\0';
        deleted_key[i] = 'F';
    }
    printf("total=%lld, count=%lld, rest=%lld", total, count, rest);
    for(i = 0; i <= count; i++){
        if(i < count){
            key_count = REDISCOUNTER_RDB_BLOCK / state.entry_size;
            nkey_read = REDISCOUNTER_RDB_BLOCK;
            if (fread_check(buf,REDISCOUNTER_RDB_BLOCK,1,fp) == 0) {
                sdsfree(val);
                //report error
            }
        }
        else{
            key_count = rest / self.entry_size;
            nkey_read = rest;
            if (fread_check(buf,rest,1,fp) == 0) {
                sdsfree(val);
                //report error
            }
        }
        //time used...
        for(j = 0; j < key_count;){

        }
    }
}
