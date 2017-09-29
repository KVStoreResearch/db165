#include <string.h>

#include "cs165_api.h"
#include "utils.h"

// currently active database - only need to support one at a time
Db* current_db;

/*
 * create_column(char* name, Table* table, bool sorted, Status* ret_status)
 * Creates a new column and adds it to the table specified.
 * - name: Name of the newly created column. Must be unique within a table.
 * - table: Pointer to the table where the column will be added.
 * - sorted: Boolean value indicates whether or not the column is sorted.
 * - ret_status: Stores the return status of the function; if any error occurs it 
 *		stores it as ERROR, otherwise OK
 * Returns a pointer to the newly created column. TODO Is this needed?
 */
Column* create_column(char* name, Table* table, bool sorted, Status* ret_status) {
	Column* new_col = (Column*) malloc(sizeof(Column));
	strncpy(new_col->name, name, MAX_SIZE_NAME);

	Column** col_slot = &table->columns;
	int cols_used = 0;
	while (cols_used < table->col_count && *col_slot != NULL) *col_slot++;
	if (cols_used >= table->col_count) {
		ret_status->code = ERROR;
		return NULL;
	}
	*col_slot = new_col;

	log_info("COLUMN CREATED in TABLE %s:\nNAME: %s\n", table->name, name);
	ret_status->code = OK;

	return new_col;
}

/* 
 * create_table(Db* db, const char* name, size_t num_columns, Status* ret_stats) 
 * Creates a new table with num_columns columns and adds it the specified database.
 * - db: Pointer to the db to which the table will be added
 * - name: Name of the table
 * - num_columns: Number of columns in the table
 * - ret_status: Stores the return status of the function; if any error occurs it 
 *		stores it as ERROR, otherwise OK
 * Returns a pointer to the newly created table. TODO Is this needed?
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status* ret_status) {
	Table* new_table = (Table*) malloc(sizeof(Table));
	if (!new_table) {
		ret_status->code = ERROR;
		return NULL;
	}

	new_table->columns = (Column*) malloc(sizeof(Column) * num_columns);
	if (!new_table->columns) {
		ret_status->code = ERROR;
		return NULL;
	}

	new_table->col_count = num_columns;
	strncpy(new_table->name, name, MAX_SIZE_NAME);
	new_table->table_length = 0;

	if (db->tables_size < db->tables_capacity) { //find new slot for the pointer to the new table
		Table** tableSlot = &db->tables;
		*tableSlot += db->tables_size;
		*tableSlot = new_table;
		db->tables_size++;
	} else {
		ret_status->code = ERROR; //no more new tables may be created
		return NULL;
	}
	
	log_info("TABLE CREATED in DB %s:\nNAME: %s\nNUMBER OF COLUMNS: %zu\n", 
			db->name, new_table->name, new_table->col_count); 
	ret_status->code = OK;
	return new_table;
}

/* 
 * add_db(const char* db_name, bool new)
 * Adds a new database to the current process.  If the database is new, then directs flow to 
 * create_db.  If it is not, then directs flow to load_db.
 * - name: Name of the database to be created or loaded
 * - new: Indicates if the database is to be created or loaded
 * Returns the status of the operation
 */
Status add_db(const char* db_name, bool new) {
	if (new) {
		return create_db(db_name);
	}

	return load_db(db_name);
}

/*
 * create_db(const char* db_name)
 * Creates a new database and sets it as the currently active database
 * - db_name: The name of the newly created database
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status create_db(const char* db_name) {
	struct Status ret_status;

	Db* new_db = (Db*) malloc(sizeof(Db));
	if (!new_db) {
		ret_status.code = OK;
		return ret_status;
	}

	new_db->tables_size = 0;
	new_db->tables_capacity = MAX_NUM_TABLES;
	strncpy(new_db->name, db_name, MAX_SIZE_NAME);
	new_db->tables = (Table*) malloc(sizeof(Table) * new_db->tables_capacity);
	current_db = new_db;

	log_info("DB CREATED:\nNAME: %s\n", new_db->name);

	ret_status.code = OK;
	return ret_status;
}

/* load_db(const char* db_name)
 * Loads a persisted database from disk. Format of the file should be a csv
 * - db_name: The name of the database to be loaded
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status load_db(const char* db_name) {
	struct Status ret_status;

	log_info("DB CREATED:\nNAME: %s\n", current_db->name); // 

	ret_status.code = OK;
	return ret_status;
}
