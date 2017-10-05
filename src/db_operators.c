#include "client_context.h"
#include "db_operators.h"
#include "utils.h"

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 **/
char* execute_db_operator(DbOperator* query) {
	if (!query) 
		return "165";

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

	return "OK";
}

char* execute_create_tbl(DbOperator* query) {
	char* table_name = query->operator_fields.create_operator.name;
	int col_count = query->operator_fields.create_operator.column_count;
    Status ret_status = create_table(current_db, table_name, col_count);

    if (ret_status.code != OK) {
        return "Could not complete create(tbl) query";
    }

	return "OK";
}

char* execute_create_col(DbOperator* query) {
	char* name = query->operator_fields.create_operator.name;
	Table* table = query->operator_fields.create_operator.table;

    Status ret_status = create_column(name, table, false);
    if (ret_status.code != OK) {
        return "Could not complete create(col) query";
    }

	return "OK";
}

char* execute_open(DbOperator* query) {
	char* filename = query->operator_fields.open_operator.filename;
	Status ret_status = load(filename); 
	if (ret_status.code != OK) {
		return "Could not load db";
	}
	
	return "OK";
}

char* execute_insert(DbOperator* query) {
	Status ret_status;
	InsertOperator op = query->operator_fields.insert_operator;
	ret_status = relational_insert(op.table, op.values);
	if (ret_status.code == ERROR) {
		return ret_status.error_message;
	}
	
	return "INSERT";
}

// TODO
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

	return "SELECT";
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

	return "FETCH";
}

char* execute_shutdown() {
	Status ret_status = shutdown_database(current_db);	
	if (ret_status.code != OK) {
		return "Could not sync DB to disk.";
	}

	return "SHUTDOWN";
}

// TODO
void db_operator_free(DbOperator* query) {
	free(query);
	return;
}

