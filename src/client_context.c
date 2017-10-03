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
