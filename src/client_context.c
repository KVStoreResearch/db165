#include <string.h>

#include "client_context.h"

Table* lookup_table(char *name) {
	strsep(&name, "."); // advance name to just the table_name 
	for (size_t i = 0; i < current_db->tables_size; i++) {
		if (strcmp(current_db->tables[i].name, name) == 0) {
			return &current_db->tables[i];
		}
	}
	return NULL;
}

// TODO
Column* lookup_column(char* name) {
	return NULL;
}

int add_handle(char* handle_name, ClientContext* context, bool result) {
	if (context->chandles_in_use == context->chandle_slots) {
		// cannot attach another handle
		return -1;
	}

	GeneralizedColumnHandle new_handle;
	strncpy(new_handle.name, handle_name, strlen(handle_name));
	new_handle.generalized_column.column_type = result ? RESULT : COLUMN; 
	int next_slot = context->chandles_in_use;
	context->chandle_table[next_slot] = new_handle;

	return 0;
}
