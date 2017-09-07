#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef int keyType;
typedef int valType;

typedef struct entry {
	keyType type;
	valType val;
	struct entry* next;
} entry;

typedef struct hashtable {
	int size;
	entry** entries;
} hashtable;


int allocate(hashtable** ht, int size);
int put(hashtable* ht, keyType key, valType value);
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results);
int erase(hashtable* ht, keyType key);
int deallocate(hashtable* ht);

#endif
