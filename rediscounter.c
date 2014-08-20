#include "rediscounter.h"

long long REDISCOUNTER_RDB_BLOCK = 10240;

/* Aof class sunlei*/
typedef struct Aof{
    int index;
    char *filename;
    sds buffer;
}Aof;

int init_aof(Aof * aof_obj, int index, char *filename){
    aof_obj->index = index;
    aof_obj->filename = strdup(filename);
    if(!filename)
        goto err;
    if((aof_obj->buffer = sdsnewlen(NULL, AOF_BUFFER_SIZE)) == NULL)
        goto err;
    return COUNTER_OK;
err:
    free(aof_obj->filename);
    sdsfree(aof_obj->buffer);
    fprintf(stderr, "init_aof filename: %s error: %s\n",
            filename, strerror(errno));
    return COUNTER_ERR;
}

//time_used function to be added
int save_aof(Aof * aof_obj){
    if(aof_obj->buffer)
        return COUNTER_OK;
    FILE *fp;
    if((fp = fopen(aof_obj->filename, "ab+")) == NULL)
        goto err;
    if(fwrite(aof_obj->buffer, strlen(aof_obj->buffer), 1, fp) != 1)
        goto err;
    fclose(fp);
    return COUNTER_OK;
err:
    fclose(fp);
    fprintf(stderr, "save_aof error: %s\n",strerror(errno));
    return COUNTER_ERR;
}

int add_aof(Aof * aof_obj, char * item){
    if(aof_obj->buffer == NULL || item == NULL){
        fprintf(stderr, "add_aof error\n");
        return COUNTER_ERR;
    }
    sdscat(aof_obj->buffer, item);
    if(sdslen(aof_obj->buffer) > AOF_BUFFER_SIZE)
        save_aof(aof_obj);

    return COUNTER_OK;
}

// init all the Aof objects with aof_number and aof_filename
Aof * setAofs(){
    Aof * aof_set = (Aof *)malloc(sizeof(Aof) * aof_number);
    if(aof_number <= 0 || !aof_filename || !aof_set){
        fprintf(stderr, "wrong aof_number or aof_filename\n");
        return NULL;
    }

    char buf[1024];
    int i;
    for(i = 0; i < aof_number; i++){
        sprintf(buf, "%s.%09d", aof_filename, i);
        if(init_aof(&aof_set[i], i, buf) == COUNTER_ERR){
            return NULL;
        }
    }
    return aof_set;
}

unsigned char read_byte(FILE * fp){
    unsigned char number;
    if (fread(&number, 1, 1, fp) == 0) return -1;
    return number;
}


long long rdb_get_long(FILE * fp){
    long long number;
    unsigned char digits = read_byte(fp);
    if(digits > RDB_INVALID_LEN)
        return -1;
    if (fread(&number, digits, 1, fp) == 0) return -1;
    return number;
}

/* Load an encoded length from the DB, see the REDIS_RDB_* defines on the top
 * of this file for a description of how this are stored on disk.
 *
 * isencoded is set to 1 if the readed length is not actually a length but
 * an "encoding type", check the above comments for more info */
uint32_t rdbLoadLen(FILE *fp, int *isencoded) {
    unsigned char buf[2];
    uint32_t len;
    int type;

    if (isencoded) *isencoded = 0;
    if (fread(buf,1,1,fp) == 0) return REDIS_RDB_LENERR;
    type = (buf[0]&0xC0)>>6;
    if (type == REDIS_RDB_6BITLEN) {
        /* Read a 6 bit len */
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_ENCVAL) {
        /* Read a 6 bit len encoding type */
        if (isencoded) *isencoded = 1;
        return buf[0]&0x3F;
    } else if (type == REDIS_RDB_14BITLEN) {
        /* Read a 14 bit len */
        if (fread(buf+1,1,1,fp) == 0) return REDIS_RDB_LENERR;
        return ((buf[0]&0x3F)<<8)|buf[1];
    } else {
        /* Read a 32 bit len */
        if (fread(&len,4,1,fp) == 0) return REDIS_RDB_LENERR;
        return ntohl(len);
    }
}

/* Load an integer-encoded object from file 'fp', with the specified
 * encoding type 'enctype'. If encode is true the function may return
 * an integer-encoded object as reply, otherwise the returned object
 * will always be encoded as a raw string. */
sds rdbLoadIntegerObject(FILE *fp, int enctype, int encode) {
    unsigned char enc[4];
    long long val;

    if (enctype == REDIS_RDB_ENC_INT8) {
        if (fread(enc,1,1,fp) == 0) return NULL;
        val = (signed char)enc[0];
    } else if (enctype == REDIS_RDB_ENC_INT16) {
        uint16_t v;
        if (fread(enc,2,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8);
        val = (int16_t)v;
    } else if (enctype == REDIS_RDB_ENC_INT32) {
        uint32_t v;
        if (fread(enc,4,1,fp) == 0) return NULL;
        v = enc[0]|(enc[1]<<8)|(enc[2]<<16)|(enc[3]<<24);
        val = (int32_t)v;
    } else {
        val = 0; /* anti-warning */
        fprintf(stderr, "Unknown RDB integer encoding type\n");
        exit(1);
        return NULL;
    }

    return sdsfromlonglong(val);
}


sds rdbLoadLzfStringObject(FILE*fp) {
    unsigned int len, clen;
    unsigned char *c = NULL;
    sds val = NULL;

    if ((clen = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(fp,NULL)) == REDIS_RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;
    if ((val = sdsnewlen(NULL,len)) == NULL) goto err;
    if (fread(c,clen,1,fp) == 0) goto err;
    //lzf_decompress to be added
    //if (lzf_decompress(c,clen,val,len) == 0) goto err;
    zfree(c);
    return val;
err:
    zfree(c);
    sdsfree(val);
    return NULL;
}

sds rdbGenericLoadStringObject(FILE*fp, int encode) {
    int isencoded;
    uint32_t len;
    sds val;

    len = rdbLoadLen(fp,&isencoded);
    if (isencoded) {
        switch(len) {
            case REDIS_RDB_ENC_INT8:
            case REDIS_RDB_ENC_INT16:
            case REDIS_RDB_ENC_INT32:
                return rdbLoadIntegerObject(fp,len,encode);
            case REDIS_RDB_ENC_LZF:
                return rdbLoadLzfStringObject(fp);
            default:
                fprintf(stderr, "Unknown RDB encoding type\n");
                return NULL;
        }
    }

    if (len == REDIS_RDB_LENERR) return NULL;
    val = sdsnewlen(NULL,len);
    if (len && fread(val,len,1,fp) == 0) {
        sdsfree(val);
        return NULL;
    }
    return val;
}


sds rdbLoadStringObject(FILE *fp) {
    return rdbGenericLoadStringObject(fp,0);
}

sds rdbLoadEncodedStringObject(FILE *fp) {
    return rdbGenericLoadStringObject(fp,1);
}

/* For information about double serialization check rdbSaveDoubleValue() */
int rdbLoadDoubleValue(FILE *fp, double *val) {
    char buf[128];
    unsigned char len;

    if (fread(&len,1,1,fp) == 0) return -1;
    if(len > 252){
        fprintf(stderr, "wrong number in rdbLoadDoubleValue\n");
        return 0;
    }
    if (fread(buf,len,1,fp) == 0) return -1;
    buf[len] = '\0';
    sscanf(buf, "%lg", val);
    return 0;

}

/* check if value is prime, return 1 if it is, 0 otherwise */
int _isPrime(unsigned long value)
{
    unsigned long i;
    if (value <= 1)
        return 0;
    if (value == 2)
        return 1;
    if (value % 2 == 0)
        return 0;

    for (i = 3; i < sqrt(value) + 1; ++i)
        if (value % i == 0)
            return 0;

    return 1;
}

int init_rdb_state(rdb_state * state, FILE * fp){
    char buf[1024];
    int rdbver;
    sds sdstemp;
    double val;

    if (fread(buf,9,1,fp) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf,"REDIS",5) != 0) {
        fclose(fp);
        fprintf(stderr,"Wrong signature trying to load DB from file\n");
        return COUNTER_ERR;
    }
    /*-------redis version check-------*/
    rdbver = atoi(buf+5);
    if (rdbver >= 2 && rdbver <= 4) {
        if (!(sdstemp = rdbLoadStringObject(fp)) || rdbLoadDoubleValue(fp, (double *)&(state->offset))) {
            fclose(fp);
            fprintf(stderr, "Failed to get aof name or offset\n");
            return COUNTER_ERR;
        }
        char aofname[255];
        sprintf(aofname, "%s", sdstemp);
        state->rdb_filename = strdup(aofname);
        fprintf(stdout, "RDB position: %s-%lld\n",
                    state->rdb_filename, state->offset);
    } else if (rdbver != 1) {
        fclose(fp);
        fprintf(stderr, "Can't handle RDB format version %d\n",rdbver);
        return COUNTER_ERR;
    }

    fprintf(stdout, "Loading dict information...\n");
    /* load db size, used, deleted_slots, key_size, entry_size */
    if (rdbLoadDoubleValue(fp, &val) == -1) goto eoferr;
    state->size = (unsigned long)val;
    if (rdbLoadDoubleValue(fp, &val) == -1) goto eoferr;
    state->used = (unsigned long)val;
    if (rdbLoadDoubleValue(fp, &val) == -1) goto eoferr;
    state->deleted = (unsigned long)val;
    if (rdbLoadDoubleValue(fp, &val) == -1) goto eoferr;
    state->key_size = (unsigned int)val;
    if (rdbLoadDoubleValue(fp, &val) == -1) goto eoferr;
    state->entry_size = (unsigned int)val;
    fprintf(stdout,  "RDB info: size-%lld, used-%lld, deleted-%lld, key-size-%lld, "
                "entry-size-%lld\n", state->size, state->used, state->deleted, state->key_size, state->entry_size);
    state->value_size = state->entry_size - state->key_size;

    // check if values are valid
    if (!_isPrime(state->size) ||
        state->used > state->size ||
        state->deleted > state->size ||
        state->key_size < 1)
    {
        fprintf(stderr, "Invalid values got from RDB: size-%lld, "
                "used-%lld, deleted-%lld, key_size-%lld\n",
                state->size, state->used, state->deleted, state->key_size);
        fprintf(stderr, "Error loading from DB. Aborting now.\n");
        exit(1);
        return COUNTER_ERR;
    }

    sdsfree(sdstemp);
    return COUNTER_OK;
eoferr:
    sdsfree(sdstemp);
    fprintf(stderr, "init_rdb_state failed\n");
    return COUNTER_ERR;
}

//default format kv, need to free result, is it ok?
char * _format_kv(char *key, int value){
    char tmp[1024];
    sprintf(tmp, "%s:%d", key, value);
    return strdup(tmp);
}

int _key_hash(char * key){
    return 0;
}


void adjust_block_size(long long entry_size){
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
    REDISCOUNTER_RDB_BLOCK = entry_size * ((REDISCOUNTER_RDB_BLOCK + entry_size - 1) / entry_size);
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
}

/* rdb parse */

int rdbLoadDict(FILE *fp, rdb_state state, Aof * aof_set) {
    adjust_block_size(state.entry_size);
    long long total = state.size * state.entry_size,
            count = total / REDISCOUNTER_RDB_BLOCK,
            rest = total % REDISCOUNTER_RDB_BLOCK,
            key_count,
            read_offset = 0,
            ndeleted_key = 0,
            nother_key = 0,
            boundary = REDISCOUNTER_RDB_BLOCK;
    sds empty_key = sdsnewlen(NULL, state.key_size),
            deleted_key = sdsnewlen(NULL, state.key_size),
            buf = sdsnewlen(NULL, REDISCOUNTER_RDB_BLOCK);
    int i, j, value;
    char *key = (char *)malloc(sizeof(char) * state.key_size),
            value_buf[4],
            *tmp;
    for(i = 0; i < state.key_size; i++){
        empty_key[i] = '\0';
        deleted_key[i] = 'F';
    }
    fprintf(stdout, "total=%lld, count=%lld, rest=%lld", total, count, rest);
    for(i = 0; i <= count; i++){
        if(i < count){
            key_count = REDISCOUNTER_RDB_BLOCK / state.entry_size;
            if (fread(buf,REDISCOUNTER_RDB_BLOCK,1,fp) == 0) {
                sdsfree(buf);
                fprintf(stderr, "rdbLoadDict error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
        }
        else{
            boundary = rest;
            key_count = rest / state.entry_size;
            if (fread(buf,rest,1,fp) == 0) {
                sdsfree(buf);
                fprintf(stderr, "rdbLoadDict error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
        }
        // time used...
        for(j = 0; j < key_count && read_offset < boundary; j++){
            // get a value
            if(strncpy(value_buf, buf + read_offset, 4) == NULL){
                fprintf(stderr, "rdbLoadDict reading value_buf error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
            read_offset += 4;
            value = atoi(value_buf);
            // get a key
            if(strncpy(key, buf + read_offset, state.key_size) == NULL){
                fprintf(stderr, "rdbLoadDict reading buf error: %s\n",strerror(errno));
                return COUNTER_ERR;
            }
            read_offset += state.key_size;

            // check key
            if(strcmp(key, empty_key) == 0){
                continue;
            }
            if(strcmp(key, deleted_key) == 0){
                ndeleted_key++;
                continue;
            }
            if(value < 1 || value > 100000000){
                nother_key++;
                continue;
            }
            tmp = _format_kv(key, value);
            if(add_aof(aof_set + (_key_hash(key) % aof_number), tmp) == COUNTER_ERR)
                fprintf(stderr, "add_aof error\n");
            free(tmp);
        }
    }

    // save the data in buffer
    for(i = 0; i < aof_number; i++){
        if(save_aof(aof_set + i) == COUNTER_ERR)
            fprintf(stderr, "save_aof error\n");
    }

    free(key);
    sdsfree(empty_key);
    sdsfree(deleted_key);
    sdsfree(buf);
    return COUNTER_OK;
}


int rdbLoad(char *filename){
    if(!filename){
        fprintf(stderr, "Invalid filename\n");
        return COUNTER_ERR;
    }
    FILE * fp;

    fp = fopen(filename,"r");
    if (!fp) {
        fprintf(stderr, "Error opening %s for loading: %s\n",
            filename, strerror(errno));
        return COUNTER_ERR;
    }

    rdb_state state;
    init_rdb_state(&state, fp);
    Aof * aof_set = setAofs();
    if(!aof_set){
        fprintf(stderr, "aof_set failed\n");
    }


    // end of function
    fclose(fp);
    fp = NULL;
    return COUNTER_OK;
}

