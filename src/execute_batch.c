#define _BSD_SOURCE
#include <pthread.h>
#include <string.h>

#include "client_context.h"

#define DEFAULT_BATCH_CAPACITY 4

int init_batch(BatchOperator* batch) {
	batch->num_ops_select = 0;
	batch->ops_capacity_select = DEFAULT_BATCH_CAPACITY;
	batch->batch_select = malloc(sizeof *(batch->batch_select) * batch->ops_capacity_select);

	batch->num_ops_fetch = 0;
	batch->ops_capacity_fetch = DEFAULT_BATCH_CAPACITY;
	batch->batch_fetch = malloc(sizeof *(batch->batch_fetch) * batch->ops_capacity_fetch);
	if (!batch->batch_select || !batch->batch_fetch || !batch->batch_select->lows 
			|| !batch->batch_select->highs || !batch->batch_fetch->positions_handles)
		return -1;

	batch->num_ops_other = 0;
	batch->ops_capacity_other = DEFAULT_BATCH_CAPACITY;
	batch->other_ops = malloc(sizeof *(batch->other_ops) * batch->ops_capacity_other);
	if (!batch->other_ops)
		return -1;

	return 0;
}

BatchSelect* find_select(Column* col, BatchOperator* batch) { 
	for (int i = 0; i < batch->num_ops_select; i++) {
		if (batch->batch_select[i].column == col 
				&& batch->batch_select[i].num_ops < MAX_NUM_SHARED_SCAN)
			return &batch->batch_select[i];
	}
	return NULL;
}

BatchFetch* find_fetch(Column* col, BatchOperator* batch) {
	for (int i = 0; i < batch->num_ops_fetch; i++) {
		if (batch->batch_fetch[i].column == col)
			return &batch->batch_fetch[i];
	}
	return NULL;
}

int handle_select_operator(DbOperator* op, BatchOperator* batch) {
	SelectOperator select_op = op->operator_fields.select_operator;
	BatchSelect* match = find_select(select_op.column, batch);
	if (match && match->num_ops < MAX_NUM_SHARED_SCAN) {
		match->lows[match->num_ops] = select_op.low;
		match->highs[match->num_ops] = select_op.high;
		match->result_handles[match->num_ops] = malloc(strlen(select_op.result_handle));
		strcpy(match->result_handles[match->num_ops], select_op.result_handle);
		match->num_ops++;
		return 0;
	}

	if (batch->num_ops_select == batch->ops_capacity_select) {
		batch->ops_capacity_select *= 2;
		BatchSelect* new_ops = realloc(batch->batch_select, 
				sizeof *new_ops * batch->ops_capacity_select);
		if (!new_ops)
			return -1;
		batch->batch_select = new_ops;
	}
	
	BatchSelect* new_select = &batch->batch_select[batch->num_ops_select];
	new_select->context = op->context;
	new_select->column = select_op.column;
	new_select->num_ops = 1;

	new_select->lows[0] = select_op.low;
	new_select->highs[0] = select_op.high;
	new_select->result_handles[0] = malloc(strlen(select_op.result_handle));
	strcpy(new_select->result_handles[0], select_op.result_handle);
	batch->num_ops_select++;
	
	return 0;
}

int handle_fetch_operator(DbOperator* op, BatchOperator* batch) {
	FetchOperator fetch_op = op->operator_fields.fetch_operator;
	BatchFetch* match = find_fetch(fetch_op.column, batch);
	if (match && match->num_ops < MAX_NUM_SHARED_SCAN) {
		match->positions_handles[match->num_ops] = fetch_op.positions_handle;
		match->result_handles[match->num_ops] = malloc(strlen(fetch_op.result_handle));
		strcpy(match->result_handles[match->num_ops], fetch_op.result_handle);
		match->num_ops++;
		return 0;
	}

	if (batch->num_ops_fetch == batch->ops_capacity_fetch) {
		batch->ops_capacity_fetch *= 2;
		BatchFetch* new_ops = realloc(batch->batch_fetch, sizeof *new_ops * batch->ops_capacity_fetch);
		if (!new_ops)
			return -1;
		batch->batch_fetch = new_ops;
	}
	
	BatchFetch* new_fetch = &batch->batch_fetch[batch->num_ops_fetch];
	new_fetch->context = op->context;
	new_fetch->column = fetch_op.column;
	new_fetch->num_ops = 1;

	new_fetch->positions_handles[0] = fetch_op.result_handle;
	new_fetch->result_handles[0] = malloc(strlen(fetch_op.result_handle));
	strcpy(new_fetch->result_handles[0], fetch_op.result_handle);
	
	return 0;
}

int handle_other_operator(DbOperator* op, BatchOperator* batch) {
	if (batch->num_ops_other == batch->ops_capacity_other) {
		batch->ops_capacity_other *= 2;
		DbOperator** new_ops = realloc(batch->other_ops, batch->ops_capacity_other);
		if (!new_ops)
			return -1;
		batch->other_ops = new_ops;
	}
	batch->other_ops[batch->num_ops_other++] = op;
	return 0;
}

int handle_db_operator(DbOperator* op, BatchOperator* batch) {
	if (!op)
		return 0;
	int r;
	switch (op->type) {
		case SELECT:
			r = handle_select_operator(op, batch);
			if (r < 0)
				return -1;
			break;
		case FETCH:
			r = handle_fetch_operator(op, batch);
			if (r < 0)
				return -1;
			break;
		default:
			r = handle_other_operator(op, batch);
			if (r < 0)
				return -1;
			break;
	}
	return 0;
}

char* execute_batch_select(BatchSelect* batch_select) {
	Status ret_status;
	Column** result_columns = select_batch(batch_select->column, batch_select->lows, 
			batch_select->highs, batch_select->num_ops, &ret_status);
	if (ret_status.code != OK)
		return "-- Error occurred when executing batch select";

	for (int i = 0; i < batch_select->num_ops; i++) {
		GeneralizedColumnHandle* result_handle = lookup_client_handle(batch_select->context,
				batch_select->result_handles[i]);	
		if (!result_handle) {
			ret_status.code = ERROR;
			return "-- Error: could not find results vector in batch";
		}
		result_handle->generalized_column.column_pointer.column = result_columns[i];
		result_handle->generalized_column.column_type = COLUMN;
	}
	ret_status.code = OK;
	return "-- Shared select executed";
}

char* execute_batch_fetch(BatchFetch* batch_fetch) {
	Status ret_status;
	Column** position_columns = malloc(sizeof *position_columns * batch_fetch->num_ops);
	for (int i = 0; i < batch_fetch->num_ops; i++) {
		GeneralizedColumnHandle* positions_handle = lookup_client_handle(batch_fetch->context,
				batch_fetch->positions_handles[i]);	
		if (!positions_handle) {
			ret_status.code = ERROR;
			return "-- Error: could not find positions vector in batch";
		}
		position_columns[i] = positions_handle->generalized_column.column_pointer.column;
	}

	Column** result_columns = fetch_batch(batch_fetch->column, position_columns, 
			batch_fetch->num_ops, &ret_status);
	if (ret_status.code != OK)
		return "-- Error occurred when executing batch fetch";

	for (int i = 0; i < batch_fetch->num_ops; i++) {
		GeneralizedColumnHandle* result_handle = lookup_client_handle(batch_fetch->context,
				batch_fetch->result_handles[i]);	
		if (!result_handle) {
			ret_status.code = ERROR;
			return "-- Error: could not find results vector in batch";
		}
		result_handle->generalized_column.column_pointer.column = result_columns[i];
		result_handle->generalized_column.column_type = COLUMN;
	}
	ret_status.code = OK;
	return "-- Shared fetch executed";
}

// pthread routine for batch select
void* run_batch_select(void* batch_select) {
	execute_batch_select((BatchSelect*) batch_select);
	pthread_exit(NULL);
}

// pthread routine for batch fetch 
void* run_batch_fetch(void* batch_fetch) {
	execute_batch_fetch((BatchFetch*) batch_fetch);
	pthread_exit(NULL);
}

// pthread routine for other operator execution 
void* run_batch_other(void* other) {
	execute_db_operator((DbOperator*) other);
	pthread_exit(NULL);
}

char* execute_db_batch(BatchOperator* batch) {
	pthread_t select_threads[MAX_NUM_THREADS];
	for (int i = 0; i < batch->num_ops_select; i+= MAX_NUM_THREADS) {
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			pthread_create(&select_threads[j % MAX_NUM_THREADS], NULL, run_batch_select, 
					(void*) &batch->batch_select[i + j]);
		}
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			int r = pthread_join(select_threads[j % MAX_NUM_THREADS], NULL);
			if (r != 0)
				return "-- Error occurred in batch execution";
		}
	}

	pthread_t fetch_threads[batch->num_ops_fetch];
	for (int i = 0; i < batch->num_ops_fetch; i += MAX_NUM_THREADS) {
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			pthread_create(&fetch_threads[j % MAX_NUM_THREADS], NULL, run_batch_fetch, 
					(void*) &batch->batch_fetch[i + j]);
		}
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			int r = pthread_join(fetch_threads[j % MAX_NUM_THREADS], NULL);
			if (r != 0)
				return "-- Error occurred in batch execution";
		}
	}

	pthread_t other_threads[batch->num_ops_other];
	for (int i = 0; i < batch->num_ops_other; i += MAX_NUM_THREADS) {
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			pthread_create(&other_threads[j % MAX_NUM_THREADS], NULL, run_batch_other, 
					(void*) batch->other_ops[i + j]);
		}
		for (int j = 0; j < MAX_NUM_THREADS; j++) {
			int r = pthread_join(other_threads[j % MAX_NUM_THREADS], NULL);
			if (r != 0)
				return "-- Error occurred in batch execution";
		}
	}
	return "-- Batch executed!";
}

