#include <stdlib.h>
#include <stdio.h>
#include "hash_table.h"

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
	*ht = (hashtable*) malloc(sizeof(hashtable));
	if (ht == NULL) {
		return -1;
	}
	(*ht)->size = size;

	entry** entries = malloc(sizeof(void*) * size * 2);
	if (entries == NULL) {
		return -1;
	}


	(*ht)->entries = entries;
	for (int i = 0; i < size * 2; i++) {
		(*ht)->entries[i] = NULL;
	}

    return 0;
}

// This method hashes a given key to a bucket.
// For now just uses the simple % ARRAY_SIZE method.
int hash(keyType key, int size) {
	return (int) key % size; 
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
	entry* newEntry = malloc(sizeof(entry));
	if (newEntry == NULL) return -1;
	newEntry->type = key;
	newEntry->val = value; 
	newEntry->next = NULL;
	
	int bucket = hash(key, ht->size);

	if (ht->entries[bucket] == NULL) {
		ht->entries[bucket] = newEntry;
	} else {
		newEntry->next = ht->entries[bucket];
		ht->entries[bucket] = newEntry;
	}

    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call. 
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
	if (!ht) return -1;

	int bucket = hash(key, ht->size);
	int nResults = 0;
	entry* currentEntry = ht->entries[bucket];
	
	while(currentEntry != NULL) {
		if (currentEntry->type == key) { 
			values[nResults] = currentEntry->val;
			nResults++;
			if (nResults > num_values) { 
				return get(ht, key, values, num_values*2, num_results);
			}
		}
		currentEntry = currentEntry->next;
	}

	(*num_results) = nResults;
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
	if (!ht) {
		return -1;
	}

	int bucket = hash(key, ht->size);
	entry** currentEntry = &ht->entries[bucket];
	entry** prevEntry = NULL;

	while (*currentEntry != NULL) {
		if ((*currentEntry)->type == key) {
			if (prevEntry != NULL) {
				(*prevEntry)->next = (*currentEntry)->next;
			}
			entry* tmp = (*currentEntry)->next;
			free(*currentEntry);
			(*currentEntry) = tmp;
		} else {
			prevEntry = currentEntry;
			*currentEntry = (*currentEntry)->next;
		}
	}
	
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    // This line tells the compiler that we know we haven't used the variable
    // yet so don't issue a warning. You should remove this line once you use
    // the parameter.
    (void) ht;
    return 0;
}
