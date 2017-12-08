#include <pthread.h>
#include <string.h>

#include "cs165_api.h"
#include "db_core_utils.h"
#include "hash_table.h"
#include "index.h"
#include "join.h"

#define DEFAULT_PARTITION_SIZE 1024
#define DEFAULT_NUM_PARTITIONS 16

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

Column** hash_partition(Column* positions_a, Column* positions_b, Column* values_a, Column* values_b, 
		int num_partitions, Status* status) {
	Column** result = malloc(sizeof *result * num_partitions * 4);
	if (!result) {
		status->code = ERROR;
		return NULL;
	}

	// allocate result partition arrays
	for (int i = 0; i < num_partitions * 4; i++)  {
		result[i] = malloc(sizeof *result[i]);
		if (!result[i]) {
			status->code = ERROR;
			return NULL;
		}
		result[i]->length = 0;

		result[i]->data  = (i / 2) % 2 == 0 ? malloc(sizeof *result[i]->data * positions_a->length) 
			: malloc(sizeof *result[i]->data * positions_b->length);
		if (!result[i]->data) {
			status->code = ERROR;
			return NULL;
		}
	}

	for (int i = 0; i < values_a->length; i++) {
		int h = hash(values_a->data[i], num_partitions);
		Column* result_p_a = result[h*4];
		Column* result_v_a = result[h*4 + 1];
		result_p_a->data[result_p_a->length++] = positions_a->data[i];
		result_v_a->data[result_v_a->length++] = values_a->data[i];
	}

	for (int i = 0; i < values_a->length; i++) {
		int h = hash(values_b->data[i], num_partitions);
		Column* result_p_b = result[h*4 + 2];
		Column* result_v_b = result[h*4 + 3];
		result_p_b->data[result_p_b->length++] = positions_b->data[i];
		result_v_b->data[result_v_b->length++] = values_b->data[i];
	}

	for (int i = 0; i < num_partitions * 4; i++) {
		realloc_column(result[i], status);
		if (status->code == ERROR)
			return NULL;
	}
	
	return result;
}

Column** join_partition(Column* positions_a, Column* positions_b, Column* values_a, 
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

void* run_join_partition(void* join_args) {
	JoinThreadArgs* args = (JoinThreadArgs*) join_args;
	Column** result = join_partition(args->positions_a, args->positions_b, 
			args->values_a, args->values_b, args->status);	
	*(args->results) = result;
	pthread_exit(NULL);
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

	results[0]->length = 0;
	results[1]->length = 0;
	results[0]->data = malloc(sizeof *results[0]->data * positions_a->length);
	results[1]->data  = malloc(sizeof *results[1]->data * positions_a->length);
	if (!results[0]->data || !results[1]->data) {
		status->code = ERROR;
		return NULL;
	}

	int num_partitions = DEFAULT_NUM_PARTITIONS;
	Column** partitions = hash_partition(positions_a, positions_b, values_a, values_b, 
			num_partitions, status);

	Column*** thread_results = malloc(sizeof *results * num_partitions);
	JoinThreadArgs** args = malloc(sizeof *args * num_partitions);
	pthread_t join_threads[num_partitions];
	for (int i = 0; i < num_partitions; i++) {
		args[i] = malloc(sizeof *args[i]);
		args[i]->positions_a = partitions[i * 4];
		args[i]->values_a = partitions[i * 4 + 1];
		args[i]->positions_b = partitions[i * 4 + 2];
		args[i]->values_b = partitions[i * 4 + 3];
		args[i]->status = status;
		args[i]->results = &(thread_results[i]);
		pthread_create(&join_threads[i], NULL, run_join_partition, (void*) args[i]);
		if (status->code == ERROR)
			return NULL;
	}

	int r;
	for (int i = 0; i < num_partitions; i++) {
		r = pthread_join(join_threads[i], NULL);
		if (r != 0)
			return NULL;
		free(args[i]);
	}
	free(args);

	for (int i = 0; i < num_partitions; i++) {
		memcpy(&results[0]->data[results[0]->length], thread_results[i][0]->data, 
				thread_results[i][0]->length * sizeof *thread_results[i][0]->data);
		memcpy(&results[1]->data[results[1]->length], thread_results[i][1]->data, 
				thread_results[i][1]->length * sizeof *thread_results[i][1]->data);

		results[0]->length += thread_results[i][0]->length;
		results[1]->length += thread_results[i][1]->length;

		// cleanup partitions after they've been used
		for (int k = 0; k < 4; k++)
			free(partitions[i*4 + k]->data);

		free(thread_results[i][0]->data);
		free(thread_results[i][1]->data);
		free(thread_results[i][0]);
		free(thread_results[i][1]);
	}

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

	sort(results[0]->data, results[0]->length, results[1]->data, NULL);
	
	return results ? results : NULL;
}
