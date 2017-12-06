#include <stdlib.h>
#include <stdio.h>
#include "hash_table.h"

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(Hashtable** ht, int size) {
	*ht = malloc(sizeof *ht);
	if (ht == NULL) return -1;
	(*ht)->size = size;

	Entry** entries = malloc(sizeof(void*) * size);
	if (entries == NULL) return -1;
	(*ht)->entries = entries;

	for (int i = 0; i < size; i++) { //initialize hashtable bucket pointers to null
		(*ht)->entries[i] = NULL;
	}
    return 0;
}

// This method hashes a given key to a bucket.
// For now just uses the simple % ARRAY_SIZE method.
int hash(int key, int size) {
	return (int) (key < 0 ? key * -1 : key) % size; 
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(Hashtable* ht, int key, int value) {
	if (!ht) return -1;

	Entry* newEntry = malloc(sizeof *newEntry);
	if (!newEntry) 
		return -1;

	newEntry->key = key;
	newEntry->val = value; 
	newEntry->next = NULL;
	
	int bucket = hash(key, ht->size);
	newEntry->next = ht->entries[bucket];
	ht->entries[bucket] = newEntry;
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
int* get(Hashtable* ht, int key) {
	if (!ht) return NULL;

	int bucket = hash(key, ht->size);
	Entry* currentEntry = ht->entries[bucket];

	while(currentEntry != NULL) {
		if (currentEntry->key == key)
			return &currentEntry->val;
		currentEntry = currentEntry->next;
	}
    return NULL;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(Hashtable* ht, int key) {
	if (!ht) return -1;

	int bucket = hash(key, ht->size);
	if (ht->entries[bucket]->key == key) { 
		Entry* tmp = ht->entries[bucket];
		ht->entries[bucket] = ht->entries[bucket]->next;
		free(tmp);
	} else { 
		Entry* prevEntry = ht->entries[bucket];
		while (prevEntry->next != NULL && prevEntry->next->key != key) prevEntry = prevEntry->next;
		if (prevEntry->next) {
			Entry* tmp = prevEntry->next;
			prevEntry->next = prevEntry->next->next;
			free(tmp);
		}
	}
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(Hashtable* ht) {
	Entry** currentEntry;
	int nFrees = 0;
	for (int i = 0; i < ht->size; i++) {
		currentEntry = &ht->entries[i];
		while (*currentEntry != NULL) {
			Entry* tmp = (*currentEntry)->next;
			free(*currentEntry);
			*currentEntry = tmp;
			nFrees++;
		}
	}
	free(ht->entries);
	free(ht);
    return 0;
}
