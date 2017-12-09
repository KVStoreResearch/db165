#define _BSD_SOURCE
#include <string.h> 
#include <limits.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_core_utils.h"
#include "index.h"
#include "utils.h"


// currently active database - only need to support one at a time
Db* current_db;

Status db_startup() {
	Status ret_status;

	char* filename = construct_filename(SESSION_PATH, false);
	FILE* f = fopen(filename, "r"); 
	if (!f) {
		ret_status.code = ERROR;
		ret_status.error_message = "No existing database.\n";
		return ret_status;
	}
	
	char db_name[MAX_SIZE_NAME];
	if (!fgets(db_name, MAX_SIZE_NAME, f)) {
		ret_status.code = ERROR;
		ret_status.error_message = "No existing database.\n";
		return ret_status;
	}
	int last_char = strlen(db_name) - 1;
	if (db_name[last_char]== '\n') // remove trailing newline
		db_name[last_char] = '\0';

	ret_status = open_db(db_name);
	return ret_status;
}

/* sync_db(Db* db)
 * Syncs the given database to disk, writes in binary mode.
 * - db: Pointer to the database to save
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status sync_db(Db* db) {
	Status ret_status;
	
	char* filename = construct_filename(db->name, true);
	FILE* f = fopen(filename, "wb");
	if (!f) {
		ret_status.code = ERROR;
		ret_status.error_message = "Could not open file for syncing database\n";
		return ret_status;
	}

	fwrite(db, sizeof(Db), 1, f);
	for (size_t i = 0; i < db->tables_size; i++) {
		fwrite(&db->tables[i], sizeof(Table), 1, f);
		for (size_t j = 0; j < db->tables[i].columns_size; j++) {
			fwrite(&db->tables[i].columns[j], sizeof(Column), 1, f);
			size_t num_rows = db->tables[i].columns[j].length;
			if (db->tables[i].columns[j].index) { // sync index if column has one
				fwrite(db->tables[i].columns[j].index, sizeof(ColumnIndex), 1, f);

				if (!db->tables[i].columns[j].clustered)
					fwrite(db->tables[i].columns[j].index->positions, sizeof(int), num_rows, f);

				fwrite(db->tables[i].columns[j].index->data[0], sizeof(int), num_rows, f);
				if (db->tables[i].columns[j].clustered)
					for (size_t k = 1; k < db->tables[i].columns_size; k++)
						fwrite(db->tables[i].columns[j].index->data[k], sizeof(int), num_rows, f);

			}
			fwrite(db->tables[i].columns[j].data, sizeof(int), num_rows, f);
		}
	}
	fclose(f);
	
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

	Db* new_db = malloc(sizeof *new_db);
	if (!new_db) {
		ret_status.code = OK;
		return ret_status;
	}

	new_db->tables_size = 0;
	new_db->tables_capacity = MAX_NUM_TABLES;
	strncpy(new_db->name, db_name, MAX_SIZE_NAME);
	new_db->tables = malloc(sizeof *new_db->tables * new_db->tables_capacity);
	current_db = new_db;

	log_info("DB CREATED:\nNAME: %s\n", new_db->name);

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
 * Returns the status of the operation.
 */
Status create_table(Db* db, const char* name, size_t num_columns) {
	Status ret_status;

	if (db->tables_size == db->tables_capacity) {
		ret_status.code = ERROR; //no more new tables may be created
		return ret_status;
	}

	Table new_table;
	strncpy(new_table.name, name, MAX_SIZE_NAME); 
	new_table.columns_capacity = num_columns;
	new_table.columns_size = 0;
	new_table.length = 0;
	
	new_table.columns = malloc(sizeof(Column) * num_columns);
	if (!new_table.columns)  {
		ret_status.code = ERROR;
		return ret_status;
	}

	db->tables[db->tables_size] = new_table;
	db->tables_size++;
	
	log_info("TABLE CREATED in DB %s:\nNAME: %s\nNUMBER OF COLUMNS: %zu\n", 
			db->name, new_table.name, new_table.columns_capacity); 
	ret_status.code = OK;

	return ret_status;
}

/*
 * create_column(char* name, Table* table, bool sorted, Status* ret_status)
 * Creates a new column and adds it to the table specified.
 * - name: Name of the newly created column. Must be unique within a table.
 * - table: Pointer to the table where the column will be added.
 * - sorted: Boolean value indicates whether or not the column is sorted.
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status create_column(char* name, Table* table, bool sorted) {
	(void) sorted;
	Status ret_status;
	if (table->columns_size == table->columns_capacity) {
		ret_status.code = ERROR;
		log_err("Error: cannot create column.\n");
		log_err("The maximum number of columns in table %s has been reached already.\n", 
				table->name);
		return ret_status;
	}

	Column new_column;
	strncpy(new_column.name, name, MAX_SIZE_NAME);
	new_column.data = malloc(COLUMN_BASE_CAPACITY * sizeof *new_column.data);
	new_column.capacity = COLUMN_BASE_CAPACITY;
	new_column.length = 0;
	new_column.num_updated = 0;
	new_column.num_deleted = 0;
	new_column.stale_index = false;
	new_column.index = NULL;
	table->columns[table->columns_size] = new_column;
	table->columns_size++;

	log_info("COLUMN CREATED in TABLE %s:\nNAME: %s\n", table->name, name);
	ret_status.code = OK;

	return ret_status;
}

Status create_index(Column* col, IndexType type, bool clustered) {
	Status ret_status;
	ColumnIndex* index = malloc(sizeof *index);
	if (!index) {
		log_err("Could not allocate memory for column index.\n");
		ret_status.code = ERROR;
		return ret_status;
	}
	index->type = type;
	index->tree = NULL;
	index->data = NULL;
	index->positions = NULL;

	col->index = index; 
	col->clustered = clustered;

	log_info("INDEX CREATED in COLUMN %s\n", col->name);	
	ret_status.code = OK;
	return ret_status;
}

/* open_db(const char* db_name)
 * Opens a persisted database from disk. File is written in binary mode.
 * - db_name: The name of the database to be loaded
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */
Status open_db(char* db_name) {
	Status ret_status;	

	char* filename = construct_filename(db_name, true);
	FILE* f = fopen(filename, "rb");
	if (!f) {
		ret_status.code = ERROR;
		ret_status.error_message = "Could not load file for reading database\n";
		return ret_status;
	}
	
	current_db = malloc(sizeof *current_db);
	fread(current_db, sizeof(Db), 1, f);	
	current_db->tables = malloc(sizeof(Table) * current_db->tables_capacity);

	for (size_t i = 0; i < current_db->tables_size; i++) {
		Table* current_table = &current_db->tables[i];
		fread(current_table, sizeof(Table), 1, f);	
		size_t num_columns = current_table->columns_size;
		current_db->tables[i].columns = malloc(sizeof(Column) * num_columns);
		for (size_t j = 0; j < num_columns; j++) {
			Column* current_column = &current_table->columns[j];
			fread(current_column, sizeof(Column), 1, f);

			size_t column_length = current_column->length;
			size_t column_capacity = current_column->capacity;

			if (current_column->index) {
				ColumnIndex* index = malloc(sizeof *index);
				fread(index, sizeof *index, 1, f);

				if (!current_column->clustered) {
					int* positions = malloc(sizeof(int) * column_capacity);
					fread(positions, sizeof *index->positions, column_length, f);
					index->positions = positions;
				} else {
					index->positions = NULL;
				}

				int** data = NULL;
				if (current_column->clustered) {
					index->data = malloc(sizeof *data * current_table->columns_size);
					for (size_t k = 0; k < current_table->columns_size; k++) {
						index->data[k] = malloc(sizeof *index->data[k] * column_capacity);
						fread(index->data[k], sizeof *index->data[k], column_length, f);
					}
				} else {
					index->data = malloc(sizeof *data); 
					index->data[0] = malloc(sizeof *index->data[0] * column_capacity);
					fread(index->data[0], sizeof *index->data[0], column_length, f);
				}
				current_column->index = index;
			}

			current_column->data = malloc(column_capacity * sizeof *current_column->data);
			fread(current_db->tables[i].columns[j].data, sizeof *current_column->data, column_length, f);


			if (current_column->index && current_column->index->type == BTREE) 
				construct_btree_index(current_column);
		}
	}
	fclose(f);
	
	ret_status.code = OK;
	return ret_status;
}

Status relational_insert(Table* table, int* values) {
	Status ret_status;
	for (size_t i = 0; i < table->columns_size; i++) {
		Column* column = &table->columns[i];
		if (column->length == column->capacity) {
			int* new_data = realloc(column->data, column->capacity * 2 * sizeof *new_data);
			if (!new_data) {
				ret_status.code = ERROR;
				return ret_status;
			}
			column->data = new_data;
			column->capacity *= 2;
		}
		column->data[table->length] = values[i];
		column->length++;

		if (column->index)
			column->stale_index = true;
	}	
	table->length++;
	
	ret_status.code = OK;
	return ret_status;
}

Status relational_update(Column* column, Column* positions, Table* table, int value) {
	Status ret_status;
	for (int i = 0; i < positions->length; i++) {
		column->data[positions->data[i]] = value;

		if (column->num_updated == UPDATE_BUF_SIZE) { // update buffer full, reconstruct index
			construct_index(column, table);	
			column->num_updated = 0;
		} else {
			column->stale_index = true;
			column->updated_positions[column->num_updated++] = positions->data[i];
		}
	}

	ret_status.code = OK;
	return ret_status;
}

Status relational_delete(Table* table, Column* positions) {
	Status ret_status;
	for (int i = 0; i < positions->length; i++) {
		for (int j = 0; j < table->columns_size; j++) {
			Column* col = &table->columns[j];
			if (col->num_deleted == DELETE_BUF_SIZE) {
				update_column_with_deletes(col);
				construct_index(col, table);
			} else {
				col->deleted_positions[col->num_deleted++] = positions->data[i];
			}
		}
	}

	ret_status.code = OK;
	return ret_status;
}

void select_btree(Column* col, int low, int high, Column* result, Status* status) {
	Node* current = col->index->tree->root;
	while (!current->is_leaf) {
		for (int i = 0; i <= current->length; i++)
			if ((i == 0 && low < current->vals[i]) 
					|| (i > 0 && i < current->length && low >= current->vals[i-1] 
						&& low < current->vals[i])
					|| (i == current->length && low >= current->vals[i])) {
				current = current->children[i];
				break;
			}
	}

	int* start_point = NULL;
	for (int i = 0; i <= current->length; i++) {
		if ((i < current->length && i < current->length - 1 && low >= current->vals[i] 
					&& low < current->vals[i+1])
				|| (i == current->length && low >= current->vals[i])
				|| (i == 0 && low < current->vals[i])) {
			start_point = (int*) current->children[i];
			while (*start_point < low) {
				start_point++;
			}
			break;
		}
	}
	if (!start_point) { // no results
		result->length = 0; 
		return;
	}

	int j = 0;
	int position_ix =  start_point - col->index->data[0];
	while (*(start_point + j) < high) {
		int p = col->clustered ? position_ix + j : col->index->positions[position_ix + j];
		result->data[j] = p;
		j++;
	}
	
	result->length = j;
	status->code = OK;
	return;
}

void select_sorted(Column* col, int low, int high, Column* result, Status* status) {
	int* sorted_copy = col->index->data[0];
	int lo = 0; 
	int hi = col->length - 1;
	int mid;

	while (lo <= hi) {
		mid = (lo + hi) / 2;
		if (sorted_copy[mid] == low) {
			break;
		} else if (low > sorted_copy[mid]) {
			lo = mid + 1;
		} else {
			hi = mid - 1;
		}
	}

	int j = mid;
	while (high > sorted_copy[j])
		j++;

	for (int i = 0; i < j - mid; i++) {
		result->data[i] = col->clustered ? mid + i : col->index->positions[mid + i];
	}

	result->length = j - mid;
	status->code = OK;
	return;
}

void select_index(Column* col, int low, int high, Column* result, Status* status) {
	if (col->stale_index) {
		Table* table = table_for_column(col);
		construct_index(col, table);
	}

	switch (col->index->type) {
		case SORTED:
			select_sorted(col, low, high, result, status);
			break;
		case BTREE:
			select_btree(col, low, high, result, status);
			break;
		default:
			log_err("Could not perform select using index; unspecified index type.\n");
			status->code = ERROR;
			break;
	}

	sort(result->data, result->length, NULL, NULL);
	sort(col->updated_positions, col->num_updated, NULL, NULL);

/*	for (int i = 0; i < result->length; i++) {
		for (int j = 0; j < col->num_updated; j++) {
			if (result->data[i] == col->updated_positions[j] 
					&& !(col->data[result->data[i]] >= low && col->data[result->data[i]] < high)) {
				for (int k = 0; k < result->length - 1; k++) 
					result->data[i] = result->data[i+1];
				i--;
				result->length--;
			}
		}
	}
	*/
	status->code = OK;
	return;
}

void update_result_with_deletes(Column* result, int* deleted, int num_deleted) {
	for (int i = 0; i < result->length; i++) {
		for (int j = 0; j < num_deleted; j++) {
			if (result->data[i] == deleted[j]) {
				for (int k = i; k < result->length - 1; k++)
					result->data[k] = result->data[k+1];
				result->length--;
				i--;
				break;
			}
		}
	}
}

Column* select_all(Column* col, int low, int high, Status* status) {
	Column* result = malloc(sizeof(*result));
	if (!result) {
		status->code = ERROR;
		return NULL;
	}

	result->data = malloc(sizeof(int) * col->capacity); 
	result->capacity = col->capacity;
	if (!result->data) {
		status->code = ERROR;
		return NULL;
	}
	int result_length = 0;

	if (col->index) {
		select_index(col, low, high, result, status);
		if (status->code != OK) 
			return NULL;
	} else {
		for (size_t i = 0; i < col->length; i++) {
			if (col->data[i] >= low && col->data[i] < high) {
				result->data[result_length++] = i;
			}
		}
		result->length = result_length;
	}
	realloc_column(result, status);
	if (status->code == ERROR)
		return NULL;

	update_result_with_deletes(result, col->deleted_positions, col->num_deleted);

	return result;
}

Column* select_fetch(Column* positions, Column* values, int low, int high, Status* status) {
	Column* result = malloc(sizeof *result);
	if (!result) {
		status->code = ERROR;
		return NULL;
	}

	result->data = malloc(sizeof(int) * positions->capacity); 
	if (!result->data) {
		status->code = ERROR;
		return NULL;
	}
	result->capacity = positions->capacity;
	int result_length = 0;

	for (size_t i = 0; i < positions->length; i++) {
		if (values->data[i] >= low && values->data[i] < high) {
			result->data[result_length++] = positions->data[i];
		}
	}
	result->length = result_length;
	realloc_column(result, status);
	if (status->code == ERROR)
		return NULL;

	return result;
}

Column* fetch(Column* col, Column* positions, Status* status) {
	Column* result = malloc(sizeof *result);
	if (!result) {
		status->code = ERROR;
		return NULL;
	}

	result->data = malloc(sizeof(int) * positions->capacity); 
	if (!result->data) {
		status->code = ERROR;
		return NULL;
	}
	result->capacity = col->capacity;
	size_t result_length = 0;

	for (size_t i = 0; i < positions->length; i++) {
		result->data[result_length++] = col->data[positions->data[i]];
	}
	result->length = result_length;
	realloc_column(result, status);
	if (status->code == ERROR)
		return NULL;

	return result;
}

Status shutdown_database(Db* db) {
	Status ret_status;
	if (!db) {
		ret_status.code = ERROR;
		return ret_status;
	}

	char* filename = construct_filename(SESSION_PATH, false);
	FILE* f = fopen(filename, "w"); 
	if (!f) {
		ret_status.code = ERROR;
		ret_status.error_message = "Cannot open file to save db.\n";
		return ret_status;
	}
	if (fwrite(db->name, strlen(db->name), 1, f) == 0) {
		ret_status.code = ERROR;
		ret_status.error_message = "Cannot write db name to session file.\n";
		return ret_status;
	}

	ret_status = sync_db(db);
	if (ret_status.code == ERROR) {
		log_err("Error while syncing db to disk.\n");
		return ret_status;
	}

	free_db(db);
	ret_status.code = OK;
	return ret_status;
}

void free_db(Db* db) {
	for (size_t i = 0; i < db->tables_size; i++) {
		for (size_t j = 0; j < db->tables[i].columns_size; j++) {
			free(db->tables[i].columns[j].data); //free data array
			//free(db->tables[i].columns[j].index);		
		}
		free(db->tables[i].columns);
	}
	free(db->tables);
	free(db);
	return;	
}

/* load_db_text(const char* db_name)
 * Loads a persisted database from disk. Format of the file should be a csv
 * - db_name: The name of the database to be loaded
 * Returns the status of the operation; status.code = OK on success, ERROR on failure
 */

Status load(char* header_line, int* data, int data_length) {
	Status ret_status;

	// read in first line with db, table, col names and construct db
	char* header_line_copy = strdup(header_line);
	char* table_name = NULL, *token = NULL;
	int num_cols = 0;	

	while ((token = strsep(&header_line_copy, ","))) {
		char* column_token = NULL;
		int arg_index = 0;
		while ((column_token = strsep(&token, "."))) {
			if (!table_name && arg_index == 1) {
				table_name = column_token;
				break;
			}
			arg_index++;
		}
		num_cols++;
	}

	// lookup table
	Table* table = NULL;
	for (size_t i = 0; i < current_db->tables_size; i++) {
		if (strcmp(current_db->tables[i].name, table_name) == 0) {
			table = &current_db->tables[i];
		}
	}
	if (!table) {
		ret_status.code = ERROR;
		return ret_status;
	}

	// load data line by line
	int row[num_cols];
	for (int i = 0; i < data_length; i += num_cols) {
		for (int j = 0; j < num_cols; j++) {
			row[j] = data[i+j]; 
		}
		ret_status = relational_insert(table, row);	
		if (ret_status.code != OK) {
			log_err("Error loading database.\n");
			return ret_status;
		}
	}

	// construct indices if necessary
	for (int i = 0; i < num_cols; i++) {
		if (table->columns[i].index) { // need to construct index on data
			construct_index(&table->columns[i], table);
		}
	}


	log_info("DB LOADED:\nNAME: %s\n", current_db->name); // 
	ret_status.code = OK;

	return ret_status;
}

double average_column(Column* column)  {
	double average = 0;
	for (int i = 0; i < (int) column->length; i++) {
		average += column->data[i];
	}
	average /= column->length;
	return average; 
}

long sum_column(Column* column) {
	long sum = 0;
	for (int i = 0; i < (int) column->length; i++) {
		sum += column->data[i];
	}
	return sum; 
}

int min_column(Column* column) {
	int min = INT_MAX;
	for (size_t i = 0; i < column->length; i++) {
		if (column->data[i] < min)
			min = column->data[i];
	}
	return min;
}

int max_column(Column* column) {
	int max = INT_MIN;
	for (size_t i = 0; i < column->length; i++) {
		if (column->data[i] > max)
			max = column->data[i];
	}
	return max;
}
