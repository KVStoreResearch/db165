#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef struct Entry {
	int key;
	int val;
	struct Entry* next;
} Entry;

typedef struct Hashtable {
	int size;
	Entry** entries;
} Hashtable;

int hash(int key, int size);

int allocate(Hashtable** ht, int size);

int put(Hashtable* ht, int key, int value);

int* get(Hashtable* ht, int key);

int erase(Hashtable* ht, int key);

int deallocate(Hashtable* ht);

#endif
