#include "cs165_api.h"
#include "db_core_utils.h"
#include "hash_table.h"

#define PARTITION_SIZE 1024

Column** join_nested_loop(Column* positions_a, Column* positions_b, Column* values_a, 
		Column* values_b, Status* status) {
	Column** results = malloc(sizeof *results * 2);
	Column* result_a = malloc(sizeof *result_a);
	Column* result_b = malloc(sizeof *result_b);
	if (!results || !result_a || !result_b) {
		status->code = ERROR;
		return NULL;
	}

	result_a->data = malloc(sizeof *result_a->data * positions_a->length);
	result_b->data  = malloc(sizeof *result_b->data * positions_a->length);
	if (!result_a->data || !result_b->data) {
		status->code = ERROR;
		return NULL;
	}

	int result_ix = 0;
	for (size_t i = 0; i < positions_a->length; i++) {
		for (size_t j = 0; j < positions_b->length; j++) {
			if (values_a->data[i] == values_b->data[j]) {
				result_a->data[result_ix] = positions_a->data[i];
				result_b->data[result_ix++] = positions_b->data[j];
			}
		}
	}
	result_a->length = result_ix;
	result_b->length = result_ix;

	realloc_column(result_a, status);
	realloc_column(result_b, status);

	if (status->code == ERROR) {
		return NULL;
	}

	results[0] = result_a;
	results[1] = result_b;
	return results;
}

Column** join_hash(Column* positions_a, Column* positions_b, Column* values_a, 
		Column* values_b, Status* status) {
	Column** results = malloc(sizeof *results * 2);
	if (!results) {
		status->code = ERROR;
		return NULL;
	}
	results[0] = malloc(sizeof *results[0]);
	results[1] = malloc(sizeof *results[0]);
	if (!results[0] || !results[1]) {
		status->code = ERROR;
		return NULL;
	}

	results[0]->data = malloc(sizeof *results[0]->data * positions_a->length);
	results[1]->data  = malloc(sizeof *results[1]->data * positions_a->length);
	if (!results[0]->data || !results[1]->data) {
		status->code = ERROR;
		return NULL;
	}
	
	// build hashtable
	Hashtable* ht = malloc(sizeof *ht);
	if (allocate(&ht, 500) < 0) {
		status->code = ERROR;
		return NULL;
	}

	for (size_t i = 0; i < positions_a->length; i++) {
		if (put(ht, values_a->data[i], positions_a->data[i]) < 0) {
			status->code = ERROR;
			return NULL;
		}
	}
	
	int result_ix = 0;
	int* pos_a;
	for (size_t i = 0; i < positions_b->length; i++) {
		pos_a = get(ht, values_b->data[i]);
		if (pos_a) {
			results[0]->data[result_ix] = *pos_a;	
			results[1]->data[result_ix++] = positions_b->data[i];
		}
	}
	results[0]->length = result_ix;
	results[1]->length = result_ix;

	realloc_column(results[0], status);
	if (status->code == ERROR)
		return NULL;
	realloc_column(results[1], status);
	if (status->code == ERROR)
		return NULL;

	return results;
}

Column** join(Column* positions_1, Column* positions_2, Column* values_1, Column* values_2, 
		JoinType type, Status* status) {
	// positions, values A are smaller than positions, values B
	Column* positions_a = positions_1->length <= positions_2->length ? positions_1 : positions_2;
	Column* positions_b = positions_a == positions_1 ? positions_2: positions_1; 

	Column* values_a = positions_a == positions_1 ? values_1 : values_2;
	Column* values_b = positions_a == positions_1 ? values_2 : values_1;

	Column** results = NULL;
	switch (type) {
		case NESTED_LOOP: 
			results = join_nested_loop(positions_a, positions_b, values_a, values_b, status);
			break;
		case HASH: 
			results = join_hash(positions_a, positions_b, values_a, values_b, status);
			break;
		default:
			status->code = ERROR;
			return NULL;
	}

	if (positions_1->length > positions_2->length) {
		Column* tmp = results[0];
		results[0] = results[1];
		results[1] = tmp;
	}
	
	return results ? results : NULL;
}
