#include "cs165_api.h"
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
	size_t column_count = query->operator_fields.create_operator.column_count;

    Status ret_status = create_column(name, table, column_count);
    if (ret_status.code != OK) {
        return "Could not complete create(col) query";
    }

	return "OK";
}

char* execute_open(DbOperator* query) {
	char* db_name = query->operator_fields.open_operator.db_name;
	Status ret_status = load_db_bin(db_name); 
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
	return "SELECT";
}

char* execute_shutdown() {
	Status ret_status = shutdown_database(current_db);	
	if (ret_status.code != OK) {
		return "Could not sync DB to disk.";
	}

	return "OK";
}

// TODO
void db_operator_free(DbOperator* query) {
	free(query);
	return;
}

