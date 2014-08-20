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

int _key_hash(char * key, int value){
    return 0;
}

/* rdb parse */
rdb_state state;

void adjust_block_size(long long entry_size){
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
    REDISCOUNTER_RDB_BLOCK = entry_size * ((REDISCOUNTER_RDB_BLOCK + entry_size - 1) / entry_size);
    fprintf(stdout, "REDISCOUNTER_RDB_BLOCK=%lld\n", REDISCOUNTER_RDB_BLOCK);
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

    fclose(fp);
    fp = NULL;
    return COUNTER_OK;
}

/*
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
            if (fread(buf,REDISCOUNTER_RDB_BLOCK,1,fp) == 0) {
                sdsfree(val);
                //report error
            }
        }
        else{
            key_count = rest / self.entry_size;
            nkey_read = rest;
            if (fread(buf,rest,1,fp) == 0) {
                sdsfree(val);
                //report error
            }
        }
        //time used...
        for(j = 0; j < key_count;){

        }
    }
}
*/
