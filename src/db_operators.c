#define _BSD_SOURCE
#include <string.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_operators.h"
#include "utils.h"

#define DEFAULT_PRINT_BUFFER_SIZE 4096

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_db_operator(DbOperator* query) {
	if (!query) 
		return "-- Error";
	if (!current_db && query->type != CREATE) 
		return "-- No currently loaded DB";

	switch (query->type) {
		case CREATE:
			return execute_create(query);
		case OPEN:
			return execute_open(query);
		case INSERT:
			return execute_insert(query);
		case SELECT:
			return execute_select(query);
		case FETCH:
			return execute_fetch(query);
		case PRINT:
			return execute_print(query);
		case AVERAGE:
		case SUM:
		case MIN:
		case MAX:
			return execute_unary_aggregate(query);
		case SHUTDOWN:
			return execute_shutdown();
		default:
			break;
	}
    return "165";
}

char* execute_create(DbOperator* query) {
	switch (query->operator_fields.create_operator.type) {
		case DB:
			return execute_create_db(query);
		case TBL:
			return execute_create_tbl(query);
		case COL:
			return execute_create_col(query);
		default:
			break;
	}
	
	return "Invalid create query.";
}

char* execute_create_db(DbOperator* query) {
	char* db_name = query->operator_fields.create_operator.name;
	Status ret_status = create_db(db_name);

	if (ret_status.code != OK) {
		return "Could not complete create(db) query.";
	}

	return "-- DB created";
}

char* execute_create_tbl(DbOperator* query) {
	char* table_name = query->operator_fields.create_operator.name;
	int col_count = query->operator_fields.create_operator.column_count;
    Status ret_status = create_table(current_db, table_name, col_count);

    if (ret_status.code != OK) {
        return "Could not complete create(tbl) query";
    }

	return "-- Table created";
}

char* execute_create_col(DbOperator* query) {
	char* name = query->operator_fields.create_operator.name;
	Table* table = query->operator_fields.create_operator.table;

    Status ret_status = create_column(name, table, false);
    if (ret_status.code != OK) {
        return "Could not complete create(col) query";
    }

	return "-- Column created";
}

// TODO
char* execute_open(DbOperator* query) {
	(void) query;	
	return "";
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

// TODO fetch-select
char* execute_select(DbOperator* query) {
	Status ret_status;
	SelectOperator op = query->operator_fields.select_operator;

	Column* result_col;
	if (!op.positions) {
		result_col = select_all(op.column, op.low, op.high, &ret_status);
	} else {
		result_col = select_posn(op.column, op.positions, op.low, op.high, &ret_status);
	}

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	result_handle->generalized_column.column_pointer.column = result_col;
	result_handle->generalized_column.column_type = COLUMN;

	return "-- Select executed";
}

char* execute_fetch(DbOperator* query) {
	Status ret_status;
	FetchOperator op = query->operator_fields.fetch_operator;

	GeneralizedColumnHandle* positions_handle = lookup_client_handle(query->context, op.positions_handle);
	if (!positions_handle) {
		ret_status.code = ERROR;
		return "Error: could not find positions vector";
	}
	Column* positions_col =  positions_handle->generalized_column.column_pointer.column;
	Column* result_col = fetch(op.column, positions_col, &ret_status);

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	if (!result_handle) {
		ret_status.code = ERROR;
		return "Error: could not find results vector";
	}
	result_handle->generalized_column.column_pointer.column = result_col;
	result_handle->generalized_column.column_type = COLUMN;

	return "-- Fetch executed";
}

char* print_column(Column* column) {
	int buf_capacity = DEFAULT_PRINT_BUFFER_SIZE;
	int buf_size = 0;
	char* buf = calloc(1, buf_capacity);

	for (size_t i = 0; i < column->length; i++) {
		int len = column->data[i] < 0 ? 2 : 1;
		int data_copy = column->data[i] < 0 ? column->data[i] * -1 : column->data[i] * 1;
		data_copy /= 10;
		while (data_copy > 0) {
			len++;
			data_copy /= 10;
		}

		if (buf_size + len >= buf_capacity) { // enlarge buffer size
			char* new_buf = realloc(buf, buf_capacity * 2);
			if (!new_buf) {
				log_err("Could not reallocate bigger buffer for printing column %s.\n",
						column->name);
			}
			buf = new_buf;
			buf_capacity *= 2;
		}

		sprintf(buf + buf_size, "%d%s", column->data[i], i < column->length - 1 ? "\n" : "\0");
		buf_size += len + 1;
	}

	return buf;
}

char* print_result(Result* result) {
	char* buf = calloc(1, DEFAULT_PRINT_BUFFER_SIZE);
	switch (result->data_type) {
		case FLOAT:
			if (sprintf(buf, "%.2f", *((double*) result->payload)) < 0)
				return "Could not print result.";
			break;
		case LONG:
			if (sprintf(buf, "%lu", *((long*) result->payload)) < 0)
				return "Could not print result.";
			break;
		default: //default case is data type INT
			if (sprintf(buf, "%d", *((int*) result->payload)) < 0)
				return "Could not print result.";
			break;
	}
	
	return buf;
}

char* print(GeneralizedColumn generalized_column) {
	switch (generalized_column.column_type) {
		case COLUMN:
			return print_column(generalized_column.column_pointer.column);
		case RESULT:
			return print_result(generalized_column.column_pointer.result);
		default:
			return "Column handle has no column type";
	}
}



char* execute_print(DbOperator* query) {
	char* handle = query->operator_fields.print_operator.handle;

	//look for handle in client_context
	for (int i = 0; i < query->context->chandles_in_use; i++) {
		if (strcmp(handle, query->context->chandle_table[i].name) == 0) {
			return print(query->context->chandle_table[i].generalized_column);
		}
	}

	//look for handle in column names in current_db
	strsep(&handle, ".");
	char* table_name = strsep(&handle, ".");
	if (!table_name) 
		return "-- Could not find column/handle to print.";
	
	for (size_t i = 0; i < current_db->tables_size; i++) {
		if (strncmp(table_name, current_db->tables[i].name, strlen(table_name)) == 0) {
			for (size_t j = 0; j < current_db->tables[i].columns_size; j++) {
				if (strncmp(handle, current_db->tables[i].columns[j].name, strlen(handle)) == 0) {
					return print_column(&current_db->tables[i].columns[j]);
				}
			}
		}
	}
	return "-- Could not find column to print";
}

bool execute_unary_aggregate_column(Column* column, Result* result, OperatorType type) {
	switch(type) {
		case AVERAGE:{
			double* average = malloc(sizeof *average);
			*average = average_column(column);
			result->payload = average;
			return true;
		}
		case SUM:{
			long* sum = malloc(sizeof *sum);
			*sum = sum_column(column);
			result->payload = sum;
			return true;
		}
		case MAX:{
			int* max = malloc(sizeof *max);
			*max = max_column(column);
			result->payload = max;
			return true;
		}
		case MIN:{
			int* min = malloc(sizeof *min);
			*min = min_column(column);
			result->payload = min;
			return true;
		}
		default:
			return false;
	}
}

char* execute_unary_aggregate(DbOperator* query) {
	UnaryAggOperator op = query->operator_fields.unary_aggregate_operator;
	char* handle = op.handle;

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	if (!result_handle) {
		return "Error: could not find results vector";
	}
	Result* result = malloc(sizeof(Result));
	result->data_type = query->type == AVERAGE ? FLOAT : (query->type == SUM ? LONG : INT);
	result->num_tuples = 1;
	result->payload = result;
	result_handle->generalized_column.column_type = RESULT;
	result_handle->generalized_column.column_pointer.result = result;
	bool executed = false;


	//look for handle in client_context
	for (int i = 0; i < query->context->chandles_in_use; i++) {
		if (strcmp(handle, query->context->chandle_table[i].name) == 0) {
			executed = execute_unary_aggregate_column(
					query->context->chandle_table[i].generalized_column.column_pointer.column,
					result,
					query->type);
			break;
		}
	}

	//look for handle in column names in current_db
	strsep(&handle, ".");
	char* table_name = strsep(&handle, ".");
	if (!executed) {
		if (!table_name)
			return "-- Could not find column/handle.";

		for (size_t i = 0; i < current_db->tables_size; i++) {
			if (strncmp(table_name, current_db->tables[i].name, strlen(table_name)) == 0) {
				for (size_t j = 0; j < current_db->tables[i].columns_size; j++) {
					if (strncmp(handle, current_db->tables[i].columns[j].name, strlen(handle)) == 0) {
						executed = execute_unary_aggregate_column(
								&current_db->tables[i].columns[j],
								result,
								query->type);
						break;
					}
				}
			}
		}
	} 

	return executed ? "-- Unary aggregation executed." : "-- Could not execute unary aggregation";
}

char* execute_sum(DbOperator* query) {
	UnaryAggOperator op = query->operator_fields.unary_aggregate_operator;
	char* handle = op.handle;
	long* sum_result = NULL;

	//look for handle in client_context
	for (int i = 0; i < query->context->chandles_in_use; i++) {
		if (strcmp(handle, query->context->chandle_table[i].name) == 0) {
			sum_result = malloc(sizeof *sum_result);
			*sum_result = sum_column(query->context->chandle_table[i].generalized_column.column_pointer.column);
		}
	}

	//look for handle in column names in current_db
	strsep(&handle, ".");
	char* table_name = strsep(&handle, ".");
	if (!sum_result) {
		if (!table_name) 
			return "-- Could not find column/handle to sum.";

		for (size_t i = 0; i < current_db->tables_size; i++) {
			if (strncmp(table_name, current_db->tables[i].name, strlen(table_name)) == 0) {
				for (size_t j = 0; j < current_db->tables[i].columns_size; j++) {
					if (strncmp(handle, current_db->tables[i].columns[j].name, strlen(handle)) == 0) {
						sum_result = malloc(sizeof *sum_result);
						*sum_result = sum_column(&current_db->tables[i].columns[j]);
					}
				}
			}
		}
	}

	if (!sum_result) {
		return "-- Could not execute sum.";
	}

	GeneralizedColumnHandle* result_handle = lookup_client_handle(query->context, op.result_handle);
	if (!result_handle) {
		return "Error: could not find results vector";
	}
	Result* result = malloc(sizeof(Result));
	result->data_type = LONG;
	result->num_tuples = 1;
	result->payload = sum_result;
	result_handle->generalized_column.column_type = RESULT;
	result_handle->generalized_column.column_pointer.result = result;

	return "-- Sum executed.";
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

