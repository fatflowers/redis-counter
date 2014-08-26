redis-counter
=============
[XXXX](#jump)
##Introduce
redis-counter works for a specified version of REDIS. It's used to analysis rdb files.

##Function
redis-counter parse redis rdb file, count deleted keys, other keys and saved keys. Save key value pair into aof files.

##Format and hash
Key-value **format** saved in aof files and **hash** strategy for which file current key-value pair go can be **defined** in this handle in main.c:
    
```c

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
typedef char * format_kv_handler(void * key, int key_len, long value, void *hashed_key);

```








<span id="jump">Hello World</span>
