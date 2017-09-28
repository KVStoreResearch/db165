#include <string.h>

#include "cs165_api.h"
#include "utils.h"

// In this class, there will always be only one active database at a time
Db *current_db;

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	ret_status->code=OK;
	return NULL;
}

/* 
 * Similarly, this method is meant to create a database.
 * As an implementation choice, one can use the same method
 * for creating a new database and for loading a database 
 * from disk, or one can divide the two into two different
 * methods.
 */
Status add_db(const char* db_name, bool new) {
	if (new) {
		return create_db(db_name);
	}

	return load_db(db_name);
}

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
	current_db = new_db;

	log_info("DB CREATED:\nNAME: %s\n", new_db->name); // 

	ret_status.code = OK;
	return ret_status;
}

Status load_db(const char* db_name) {
	struct Status ret_status;

	log_info("DB CREATED:\nNAME: %s\n", current_db->name); // 

	ret_status.code = OK;
	return ret_status;
}
