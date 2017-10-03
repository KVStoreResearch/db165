#define _DEFAULT_SOURCE

#include <string.h> 
#include "cs165_api.h"
#include "utils.h"

#define MAX_LINE_SIZE 1024
#define MAX_NUM_COLUMNS 256

// currently active database - only need to support one at a time
Db* current_db;

/*
 * create_column(char* name, Table* table, bool sorted, Status* ret_status)
 * Creates a new column and adds it to the table specified.
 * - name: Name of the newly created column. Must be unique within a table.
 * - table: Pointer to the table where the column will be added.
 * - sorted: Boolean value indicates whether or not the column is sorted.
 * Returns the status of the operation.
 */
Status create_column(char* name, Table* table, bool sorted) {
	Status ret_status;
	if (table->cols_used == table->col_count) {
		ret_status.code = ERROR;
		log_err("Error: cannot create column.\n");
		log_err("The maximum number of columns in table %s has been reached already.\n", 
				table->name);
		return ret_status;
	}

	Column new_col;
	strncpy(new_col.name, name, MAX_SIZE_NAME);
	new_col.data = (int*) malloc(MAX_COL_SIZE);
	table->columns[table->cols_used] = new_col;
	table->cols_used++;

	log_info("COLUMN CREATED in TABLE %s:\nNAME: %s\n", table->name, name);
	ret_status.code = OK;

	return ret_status;
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
Status create_table(Db* db, const char* name, size_t num_columns) {
	Status ret_status;

	if (db->tables_size == db->tables_capacity) {
		ret_status.code = ERROR; //no more new tables may be created
		return ret_status;
	}

	Table new_table;
	strncpy(new_table.name, name, MAX_SIZE_NAME); 
	new_table.col_count = num_columns;
	new_table.cols_used = 0;
	new_table.table_length = 0;
	
	new_table.columns = (Column*) malloc(sizeof(Column) * num_columns);
	if (!new_table.columns)  {
		ret_status.code = ERROR;
		return ret_status;
	}

	db->tables[db->tables_size] = new_table;
	db->tables_size++;
	
	log_info("TABLE CREATED in DB %s:\nNAME: %s\nNUMBER OF COLUMNS: %zu\n", 
			db->name, new_table.name, new_table.col_count); 
	ret_status.code = OK;

	return ret_status;
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

/* load_columns(char** column_names, Table* table, int num_cols) 
 * Loads num_cols columns into table, with columns_names as names
 * - column_names: names of the columns to be loaded
 * - table: table to load columns into
 * - num_cols: number of columns to load in
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status load_columns(char** column_names, Table* table, int num_cols) {
	Status ret_status;
	for (int i = 0; i < num_cols; i++) {
		ret_status = create_column(column_names[i], table, false);
		if (ret_status.code == ERROR) {
			return ret_status;
		}
	}
	ret_status.code = OK;
	return ret_status;
}


/* load_db_header(char* db_header)
 * Loads db meta data given the first line of the csv. Creates appropriate tables,
 * columns with specified metdata.
 * - db_header: text buffer with the first line of the csv read in.
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
/*
Status load_db_header(char* db_header) {
	Status ret_status;
	char* db_header_copy = malloc(strlen(db_header));
	strncpy(db_header_copy, db_header, strlen(db_header));

	int num_columns = 0;
	char* token, *current_table_name, *table_name;
	char* column_names[MAX_NUM_COLUMNS];
	Table* current_table;

	while ((token = strsep(&db_header_copy, ","))) {

		strsep(&token, "."); // skip over the database name
		table_name = strsep(&token, ".");
		if (!current_table_name) current_table_name = table_name;

		// if we've hit a new table name, create the one we've seen thus far
		if (strcmp(table_name, current_table_name) != 0) {
			current_table = create_table(current_db, current_table_name, num_columns, &ret_status);
			load_columns(column_names, current_table, num_columns);

			current_table_name = table_name;
			num_columns = 0;
		}

		column_names[num_columns] = strsep(&token, ".");
		if (token && token[strlen(token) - 1] == '\n') token[strlen(token) - 1] = '\0';
		num_columns++;

	}
	current_table = create_table(current_db, table_name, num_columns, &ret_status);
	ret_status = load_columns(column_names, current_table, num_columns);

	ret_status.code = OK;
	return ret_status;
}*/

/* load_db_text(const char* db_name)
 * Loads a persisted database from disk. Format of the file should be a csv
 * TODO only loads first line with database metadata. 
 * - db_name: The name of the database to be loaded
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
/*
Status load_db_text(const char* db_name) {
	Status ret_status;

	char* db_filename = construct_filename(db_name);
	FILE* f = fopen(db_filename, "r");
	
	ret_status = create_db(db_name);
	if (ret_status.code == ERROR) {
		log_err("DB %s could not be loaded.", db_name);
		return ret_status;
	}

	// read in first line with db, table, col names and construct db
	char buf[MAX_LINE_SIZE];
	char* header_line = fgets(buf, MAX_LINE_SIZE, f); 
	load_db_header(header_line);

	fclose(f);

	log_info("DB LOADED:\nNAME: %s\n", current_db->name); // 
	ret_status.code = OK;

	return ret_status;
}*/

/* load_db_bin(const char* db_name)
 * Loads a persisted database from disk. File is written in binary mode.
 * - db_name: The name of the database to be loaded
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status load_db_bin(const char* db_name) {
	Status ret_status;	

	char* filename = construct_filename(db_name);
	FILE* f = fopen(filename, "rb");
	if (!f) {
		ret_status.code = ERROR;
		ret_status.error_message = "Could not load file for database\n";
		return ret_status;
	}
	
	current_db = (Db*) malloc(sizeof(Db));
	fread(current_db, sizeof(Db), 1, f);	
	current_db->tables = (Table*) malloc(sizeof(Table) * current_db->tables_capacity);

	for (size_t i = 0; i < current_db->tables_size; i++) {
		fread(&current_db->tables[i], sizeof(Table), 1, f);	
		size_t num_columns = current_db->tables[i].col_count;
		current_db->tables[i].columns = (Column*) malloc(sizeof(Column) * num_columns);

		for (size_t j = 0; j < num_columns; j++) {
			fread(&current_db->tables[i].columns[j], sizeof(Column), 1, f);
			current_db->tables[i].columns[j].data = (int*) malloc(MAX_COL_SIZE);
			size_t num_rows = current_db->tables[i].table_length;
			fread(current_db->tables[i].columns[j].data, sizeof(int), num_rows, f);
		}
	}
	fclose(f);
	
	ret_status.code = OK;
	return ret_status;
}

/* sync_db(Db* db)
 * Syncs the given database to disk, writes in binary mode.
 * - db: Pointer to the database to save
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */

Status sync_db(Db* db) {
	Status ret_status;
	
	char* filename = construct_filename(db->name);
	FILE* f = fopen(filename, "wb");

	fwrite(db, sizeof(Db), 1, f);
	for (size_t i = 0; i < db->tables_size; i++) {
		fwrite(&db->tables[i], sizeof(Table), 1, f);
		for (size_t j = 0; j < db->tables[i].col_count; j++) {
			fwrite(&db->tables[i].columns[j], sizeof(Column), 1, f);
			size_t num_rows = db->tables[i].table_length;
			fwrite(db->tables[i].columns[j].data, sizeof(int), num_rows, f);
		}
	}
	fclose(f);
	
	ret_status.code = OK;
	return ret_status;
}

Status relational_insert(Table* table, int* values) {
	Status ret_status;
	for (size_t i = 0; i < table->col_count; i++) {
		table->columns[i].data[table->table_length] = values[i];
	}	
	table->table_length++;
	
	ret_status.code = OK;
	return ret_status;
}

Status shutdown_database(Db* db) {
	struct Status ret_status;

	struct Status stat = sync_db(db);
	if (stat.code == ERROR) {
		log_err("Error while syncing db to disk.\n");
		ret_status.code = ERROR;
		return ret_status;
	}

	ret_status.code = OK;
	return ret_status;
}
