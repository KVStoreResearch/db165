#include <limits.h>

#include "cs165_api.h"

Column** select_batch(Column* col, int* lows, int* highs, int num_ops, Status* status) {
	Column** result = malloc((sizeof *result) * num_ops);
	int* result_lengths = malloc(sizeof(int) * num_ops);
	for (int i = 0; i < num_ops; i++) {
		result[i] = malloc(sizeof *result[i]);
		if (!result[i]) {
			status->code = ERROR;
			return NULL;
		}
		result[i]->data = malloc(sizeof(int) * col->capacity);
		result[i]->capacity = col->capacity;
		if (!result[i]->data) {
			status->code = ERROR;
			return NULL;
		}
		result_lengths[i] = 0;
	}

	for (size_t i = 0; i < col->length; i++) {
		for (int j = 0; j < num_ops; j++) {
			if (col->data[i] >= lows[j] && col->data[i] < highs[j]) {
				result[j]->data[result_lengths[j]++] = i;
			}
		}
	}

	for (int i = 0; i < num_ops; i++) {
		result[i]->length = result_lengths[i];
	}

	status->code = OK;
	return result;
}

Column** fetch_batch(Column* col, Column** position_columns, int num_ops, Status* status) {
	Column** result = malloc((sizeof *result) * num_ops);
	int* position_ixs = malloc(sizeof *position_ixs * num_ops);
	for (int i = 0; i < num_ops; i++) {
		result[i] = malloc(sizeof *result[i]);
		if (!result[i]) {
			status->code = ERROR;
			return NULL;
		}
		result[i]->data = malloc(sizeof(int) * position_columns[i]->capacity);
		result[i]->capacity = col->capacity;
		if (!result[i]->data) {
			status->code = ERROR;
			return NULL;
		}
		position_ixs[i] = 0;
	}

	for (int i = 0; i < (int) col->length; i++) {
		for (int j = 0; j < num_ops; j++) {
			if (i == position_columns[j]->data[position_ixs[j]]) {
				result[j]->data[position_ixs[j]++] = i;
			}
		}
	}

	for (int i = 0; i < num_ops; i++) {
		result[i]->length = position_columns[i]->length;
	}

	status->code = OK;
	return result;
}
