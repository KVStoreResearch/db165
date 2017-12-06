#define _BSD_SOURCE
#include <string.h>

#include "client_context.h"
#include "cs165_api.h"
#include "execute.h"
#include "join.h"
#include "utils.h"

#define DEFAULT_PRINT_BUFFER_SIZE 4096

GeneralizedColumnHandle* assign_column_to_handle(Column* col, char* handle, ClientContext* context) {
	GeneralizedColumnHandle* result = lookup_client_handle(context, handle);
	if (!result)
		return NULL;

	result->generalized_column.column_pointer.column = col;
	result->generalized_column.column_type = COLUMN;
	return result;
}

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_db_operator(DbOperator* query) {
	if (!query) 
		return "-- Error occurred.";
	if (!current_db && query->type != CREATE) 
		return "-- No currently loaded DB.";

	switch (query->type) {
		case CREATE:
			return execute_create(query);
		case INSERT:
			return execute_insert(query);
		case SELECT:
			return execute_select(query);
		case FETCH:
			return execute_fetch(query);
		case JOIN:
			return execute_join(query);
		case PRINT:
			return execute_print(query);
		case AVERAGE:
		case SUM:
		case MIN:
		case MAX:
			return execute_unary_aggregate(query);
		case ADD:
		case SUB:
			return execute_binary_aggregate(query);
		case SHUTDOWN:
			return execute_shutdown();
		default:
			break;
	}
    return "-- Unknown query."; 
}

char* execute_create(DbOperator* query) {
	switch (query->operator_fields.create_operator.type) {
		case DB:
			return execute_create_db(query);
		case TBL:
			return execute_create_tbl(query);
		case COL:
			return execute_create_col(query);
		case IDX:
			return execute_create_idx(query);
		default:
			break;
	}
	
	return "-- Invalid create query.";
}

char* execute_create_db(DbOperator* query) {
	char* db_name = query->operator_fields.create_operator.name;
	Status ret_status = create_db(db_name);

	if (ret_status.code != OK) {
		return "-- Could not complete create(db) query.";
	}

	return "-- DB created.";
}

char* execute_create_tbl(DbOperator* query) {
	char* table_name = query->operator_fields.create_operator.name;
	int col_count = query->operator_fields.create_operator.column_count;

    Status ret_status = create_table(current_db, table_name, col_count);
    if (ret_status.code != OK) {
        return "-- Could not complete create(tbl) query";
    }

	return "-- Table created";
}

char* execute_create_col(DbOperator* query) {
	char* name = query->operator_fields.create_operator.name;
	Table* table = query->operator_fields.create_operator.table;

    Status ret_status = create_column(name, table, false);
    if (ret_status.code != OK) {
        return "-- Could not complete create(col) query";
    }

	return "-- Column created";
}

char* execute_create_idx(DbOperator* query) {
	Column* col = query->operator_fields.create_operator.column;
	IndexType type = query->operator_fields.create_operator.idx_type; 
	bool clustered = query->operator_fields.create_operator.clustered;

	Status ret_status = create_index(col, type, clustered);	
	if (ret_status.code != OK) {
		return "-- Colud not complete create(idx) query";
	}

	return "-- Index created";
}

char* execute_insert(DbOperator* query) {
	Status ret_status;
	InsertOperator op = query->operator_fields.insert_operator;
	ret_status = relational_insert(op.table, op.values);
	if (ret_status.code == ERROR) {
		return ret_status.error_message;
	}
	
	return "-- Insert executed";
}

char* execute_select(DbOperator* query) {
	Status ret_status;
	SelectOperator op = query->operator_fields.select_operator;

	Column* result_col;
	if (!op.values) { // regular select
		result_col = select_all(op.column, op.low, op.high, &ret_status);
	} else { // fetch select
		result_col = select_fetch(op.column, op.values, op.low, op.high, &ret_status);
	}

	if (!assign_column_to_handle(result_col, op.result_handle, query->context)) {
		ret_status.code = ERROR;
		return "-- Error in assigning select result to client context";
	}

	return "-- Select executed";
}

char* execute_fetch(DbOperator* query) {
	Status ret_status;
	FetchOperator op = query->operator_fields.fetch_operator;

	GeneralizedColumnHandle* positions_handle = lookup_client_handle(query->context, op.positions_handle);
	if (!positions_handle) {
		ret_status.code = ERROR;
		return "-- Error: could not find positions vector";
	}
	Column* positions_col =  positions_handle->generalized_column.column_pointer.column;
	Column* result_col = fetch(op.column, positions_col, &ret_status);

	if (!assign_column_to_handle(result_col, op.result_handle, query->context)) {
		ret_status.code = ERROR;
		return "-- Error in assigning fetch result to client context";
	}

	return "-- Fetch executed";
}

char* execute_join(DbOperator* query) {
	Status ret_status;
	JoinOperator op = query->operator_fields.join_operator;

	GeneralizedColumnHandle* positions_1_handle = lookup_client_handle(query->context, op.positions_1);
	if (!positions_1_handle) {
		ret_status.code = ERROR;
		return "-- Error: could not find positions vector";
	}
	Column* positions_1 =  positions_1_handle->generalized_column.column_pointer.column;

	GeneralizedColumnHandle* positions_2_handle = lookup_client_handle(query->context, op.positions_2);
	if (!positions_2_handle) {
		ret_status.code = ERROR;
		return "-- Error: could not find positions vector";
	}
	Column* positions_2 =  positions_2_handle->generalized_column.column_pointer.column;

	GeneralizedColumnHandle* values_1_handle = lookup_client_handle(query->context, op.values_1);
	if (!values_1_handle) {
		ret_status.code = ERROR;
		return "-- Error: could not find positions vector";
	}
	Column* values_1 =  values_1_handle->generalized_column.column_pointer.column;

	GeneralizedColumnHandle* values_2_handle = lookup_client_handle(query->context, op.values_2);
	if (!positions_2_handle) {
		ret_status.code = ERROR;
		return "-- Error: could not find positions vector";
	}
	Column* values_2 =  values_2_handle->generalized_column.column_pointer.column;

	Column** result_columns = join(positions_1, positions_2, values_1, values_2, op.type, &ret_status);	

	if (!assign_column_to_handle(result_columns[0], op.result_1, query->context)) {
		ret_status.code = ERROR;
		return "-- Error in assigning fetch result to client context";
	}

	if (!assign_column_to_handle(result_columns[1], op.result_2, query->context)) {
		ret_status.code = ERROR;
		return "-- Error in assigning fetch result to client context";
	}

	return "-- Joined columns!";
}

int print_column(Column* column, char** buf_ptr, int* buf_size, int* buf_capacity) {
	for (size_t i = 0; i < column->length; i++) {
		int len = column->data[i] < 0 ? 2 : 1;
		int data_copy = column->data[i] < 0 ? column->data[i] * -1 : column->data[i] * 1;
		data_copy /= 10;
		while (data_copy > 0) {
			len++;
			data_copy /= 10;
		}

		if (*buf_size + len >= *buf_capacity) { // enlarge buffer size
			char* new_buf = realloc(*buf_ptr, *buf_capacity * 2);
			if (!new_buf) {
				log_err("Could not reallocate bigger buffer for printing column %s.\n",
						column->name);
			}
			*buf_ptr = new_buf;
			*buf_capacity *= 2;
		}

		sprintf(*buf_ptr + *buf_size, "%d%s", column->data[i], i < column->length - 1 ? "\n" : "\0");
		*buf_size += len + 1;
	}

	return *buf_size;
}

int print_columns(Column** columns, int num_columns, char** buf_ptr, int* buf_size,
		int* buf_capacity) {
	for (size_t i = 0; i < columns[0]->length; i++) {
		for (int j = 0; j < num_columns; j++) {
			int len = columns[j]->data[i] < 0 ? 2 : 1;
			int data_copy = columns[j]->data[i] < 0 ? columns[j]->data[i] * -1 : columns[j]->data[i] * 1;
			data_copy /= 10;
			while (data_copy > 0) {
				len++;
				data_copy /= 10;
			}

			if (*buf_size + len >= *buf_capacity) { // enlarge buffer size
				char* new_buf = realloc(*buf_ptr, *buf_capacity * 2);
				if (!new_buf) {
					log_err("Could not reallocate bigger buffer for printing column %s.\n",
							columns[i]->name);
				}
				*buf_ptr = new_buf;
				*buf_capacity *= 2;
			}

			sprintf(*buf_ptr + *buf_size, "%d", columns[j]->data[i]);
			*buf_size += len;
			if (j < num_columns - 1) {
				sprintf(*buf_ptr + *buf_size, ",");
				*buf_size += 1;
			}
		}
		sprintf(*buf_ptr + *buf_size, "%s", i < columns[0]->length - 1 ? "\n" : "\0");
		(*buf_size)++;
	}
	return *buf_size;
}

// NOTE: does not check if buffer can hold result given current buf_size and capacity
int print_results(Result** results, int num_results, char** buf_ptr, int* buf_size) {
	int r = 0;
	for (int i = 0; i < num_results; i++) {
		switch (results[i]->data_type) {
			case FLOAT:
				r = sprintf(*buf_ptr + *buf_size, "%.2f", *((double*) results[i]->payload));
				if (r < 0)
					return -1;
				break;
			case LONG:
				r = sprintf(*buf_ptr + *buf_size, "%ld", *((long*) results[i]->payload));
				if (r < 0)
					return -1;
				break;
			default: //default case is data type INT
				r = sprintf(*buf_ptr + *buf_size, "%d", *((int*) results[i]->payload));
				if (r < 0)
					return -1;
				break;
		}
		*buf_size += r;
		int s = sprintf(*buf_ptr + *buf_size, "%s", i < num_results - 1 ? "," : "\n");
		if (s < 0)
			return -1;
		*buf_size += s;
	}
	return *buf_size;
}

char* execute_print(DbOperator* query) {
	PrintOperator op = query->operator_fields.print_operator;
	int* buf_capacity = malloc(sizeof *buf_capacity);
	int* buf_size = malloc(sizeof *buf_size);
	*buf_size = 0;
	*buf_capacity = DEFAULT_PRINT_BUFFER_SIZE;
	char** buf_ptr = malloc(sizeof *buf_ptr);
	*buf_ptr = calloc(1, *buf_capacity);

	GeneralizedColumnHandle* generalized_handle = lookup_client_handle(query->context, 
			op.handles[0]);
	if (generalized_handle) {
		if (generalized_handle->generalized_column.column_type == RESULT) {
			Result** results = malloc(sizeof *results * op.num_handles);
			for (int i = 0; i < op.num_handles; i++) {
				GeneralizedColumnHandle* handle = lookup_client_handle(query->context,
						op.handles[i]);
				if (!handle) {
					free(results);
					return "-- Could not find column to print.";
				}
				results[i] = handle->generalized_column.column_pointer.result;
			}
			if (print_results(results, op.num_handles, buf_ptr, buf_size) < 0)
				return "-- Print execution failed.";
		} else if (generalized_handle->generalized_column.column_type == COLUMN) {
			Column** columns = malloc(sizeof *columns * op.num_handles);
			for (int i = 0; i < op.num_handles; i++) {
				GeneralizedColumnHandle* handle = lookup_client_handle(query->context,
						op.handles[i]);
				if (!handle) {
					free(columns);
					return "-- Could not find column to print.";
				}
				columns[i] = handle->generalized_column.column_pointer.column;
			}
			if (print_columns(columns, op.num_handles, buf_ptr, buf_size, buf_capacity) < 0)
				return "-- Print execution failed.";
		}
	} else {
		Column** columns = malloc(sizeof *columns * op.num_handles);
		for (int i = 0; i < op.num_handles; i++) {
			Column* column = lookup_column(op.handles[i]);
			if (!column) {
				free(columns);
				return "-- Could not find column to print.";
			}
			columns[i] = column;
		}
		if (print_columns(columns, op.num_handles, buf_ptr, buf_size, buf_capacity) < 0)
			return "-- Print execution failed.";
	}

	return *buf_size > 0 
		? *buf_ptr
		: "-- Could not find column to print";
}

bool execute_unary_aggregate_column(Column* column, Result* result, OperatorType type) {
	switch(type) {
		case AVERAGE:{
			double* average = malloc(sizeof *average);
			*average = average_column(column);
			result->payload = average;
			result->data_type = FLOAT;
			return true;
		}
		case SUM:{
			long* sum = malloc(sizeof *sum);
			*sum = sum_column(column);
			result->payload = sum;
			result->data_type = LONG;
			return true;
		}
		case MAX:{
			int* max = malloc(sizeof *max);
			*max = max_column(column);
			result->payload = max;
			result->data_type = INT;
			return true;
		}
		case MIN:{
			int* min = malloc(sizeof *min);
			*min = min_column(column);
			result->payload = min;
			result->data_type = INT;
			return true;
		}
		default:
			return false;
	}
}

char* execute_unary_aggregate(DbOperator* query) {
	UnaryAggOperator op = query->operator_fields.unary_aggregate_operator;

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	if (!result_handle) {
		return "Error: could not find results vector";
	}
	Result* result = malloc(sizeof(Result));
	result->num_tuples = 1;
	result->payload = result;
	result_handle->generalized_column.column_type = RESULT;
	result_handle->generalized_column.column_pointer.result = result;

	GeneralizedColumnHandle* generalized_handle = lookup_client_handle(query->context, op.handle);
	Column* column = NULL;
	if (!generalized_handle)
		column = lookup_column(op.handle);
	else 
		column = generalized_handle->generalized_column.column_pointer.column;

	return execute_unary_aggregate_column(column, result, query->type) 
		? "-- Unary aggregation executed." 
		: "-- Could not execute unary aggregation";
}

bool execute_binary_aggregate_columns(Column* column1, Column* column2, Column* result_column, 
		OperatorType type) {
	result_column->data = malloc(column1->length * sizeof *(result_column->data));
	result_column->length = column1->length;
	if (column1->length != column2->length || !result_column->data)
		return false;

	switch (type) {
		case ADD:
			for (size_t i = 0; i < column1->length; i++) {
				result_column->data[i] = column1->data[i] + column2->data[i];
			}
			break;
		case SUB:
			for (size_t i = 0; i < column1->length; i++) {
				result_column->data[i] = column1->data[i] - column2->data[i];
			}
			break;
		default:
			return false;
	}
	return true;
}

char* execute_binary_aggregate(DbOperator* query) {
	BinaryAggOperator op = query->operator_fields.binary_aggregate_operator;

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	if (!result_handle) {
		return "Error: could not find results vector";
	}
	Column* result_column = malloc(sizeof(Column));
	result_handle->generalized_column.column_type = COLUMN;
	result_handle->generalized_column.column_pointer.column = result_column;

	GeneralizedColumnHandle* generalized_handle1 = lookup_client_handle(query->context, op.handle1);
	GeneralizedColumnHandle* generalized_handle2 = lookup_client_handle(query->context, op.handle2);
	Column* column1 = NULL;
	Column* column2 = NULL;
	if (!generalized_handle1)
		column1 = lookup_column(op.handle1);
	else 
		column1 = generalized_handle1->generalized_column.column_pointer.column;
	if (!generalized_handle2)
		column2 = lookup_column(op.handle2);
	else 
		column2 = generalized_handle2->generalized_column.column_pointer.column;

	return (column1 
			&& column2 
			&& execute_binary_aggregate_columns(column1, column2, result_column, query->type))
		? "-- Binary aggregation executed."
		: "-- Could not execute binary aggregation";
}

char* execute_shutdown() {
	Status ret_status = shutdown_database(current_db);	
	if (ret_status.code != OK) {
		return "Could not sync DB to disk.";
	}
	
	return "-- DB shutdown.";
}

void db_operator_free(DbOperator* query) {
	if (!query)
		return;
	if (query->type == INSERT) {
		free(query->operator_fields.insert_operator.values);
	}

	free(query);
	return;
}

