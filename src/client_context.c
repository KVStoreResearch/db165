#define _X_OPEN_SOURCE
#include <string.h>

#include "client_context.h"

/*
 * lookup_table(char* name)
 * Returns pointer to a Table given fully qualified column name
 */
Table* lookup_table(char *name) {
	char* name_copy = strndup(name, strlen(name));
	char* db_name = strsep(&name_copy, "."); // advance name to just the table_name 
	if (strcmp(db_name, current_db->name) != 0)
		return NULL;

	for (size_t i = 0; i < current_db->tables_size; i++) {
		if (strcmp(current_db->tables[i].name, name_copy) == 0) {
			return &current_db->tables[i];
		}
	}
	return NULL;
}

/*
 * lookup_column(char* name)
 * Returns pointer to a Column given fully qualified column name
 */
Column* lookup_column(char* name) {
	char* name_copy = strndup(name, strlen(name));
	char* db_name = strsep(&name_copy, ".");
	char* table_name = strsep(&name_copy, ".");
	char* db_table_name = (char*) malloc(strlen(db_name) + strlen(table_name) + 1);
	strcpy(db_table_name, db_name);
	strcat(db_table_name, ".");
	strcat(db_table_name, table_name);

	Table* table = lookup_table(db_table_name);
	if (!table || !name_copy) 
		return NULL;
	
	for (size_t i = 0; i < table->columns_size; i++) {
		if (strcmp(table->columns[i].name, name_copy) == 0) {
			return &table->columns[i];
		}
	}

	return NULL;
}

/*
 * lookup_client_handle(char* handle_name)
 * Returns pointer to a Handle given fully qualified column name
 */
GeneralizedColumnHandle* lookup_client_handle(ClientContext* context, char* handle) {
	for (int i = 0; i < context->chandles_in_use; i++) {
		if (strcmp(context->chandle_table[i].name, handle) == 0) {
			return &context->chandle_table[i];
		}
	}
	return NULL;
}

/*
 * add_handle(ClientContext* context, char* handle, bool result)
 * Returns 0 on success, -1 on failure
 */
int add_handle(ClientContext* context, char* handle, bool result) {
	if (context->chandles_in_use == context->chandle_slots) {
		// cannot attach another handle
		return -1;
	}

	GeneralizedColumnHandle new_handle;
	for (int i = 0; i < HANDLE_MAX_SIZE; i++) {
		new_handle.name[i] = '\0';
	}
	strncpy(new_handle.name, handle, strlen(handle));
	new_handle.generalized_column.column_type = result ? RESULT : COLUMN; 
	context->chandle_table[context->chandles_in_use] = new_handle;
	context->chandles_in_use++;

	return 0;
}
