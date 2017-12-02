#include "cs165_api.h"

/**
 * sorts an array of integers
 * if with_positions = true,
 * returns vector of positions corresponding to sorted order of arr
 * else,
 * returns NULL
 */
int* sort(int* arr, int arr_len, int* positions, Table* table);

void select_index(Column* col, int low, int high, Column* result, Status* status);

Status construct_index(Column* col, Table* table);

Status construct_btree_index(Column* col);

