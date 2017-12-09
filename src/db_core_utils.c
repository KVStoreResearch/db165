#include "cs165_api.h"
#include "index.h"
#include "db_core_utils.h"

// reallocates column data to only occupy memory as big as column length 
// rather than multiple of default column capacity
// used to relieve memory pressure 
void realloc_column(Column* col, Status* status) {
	if (col->length == 0 || col->length == col->capacity)
		return;

	int* new_data = realloc(col->data, col->length * sizeof *col->data);
	if (!new_data) {
		status->code = ERROR;
		return;
	}
	col->data = new_data;
	col->capacity = col->length;
	return;
}

void update_column_with_deletes(Column* col) {
	sort(col->deleted_positions, col->num_deleted, NULL, NULL);
	for (int i = 0; i < col->num_deleted - 1; i++) {
		if (i < col->num_deleted - 1) {
			for (int j = col->deleted_positions[i]; j < col->deleted_positions[i+1]; j++) 
				col->data[j - i] = col->data[j + 1];
		}
		else {
			for (int j = col->deleted_positions[col->num_deleted-1]; j < (int) col->length; j++)
				col->data[j - i] = col->data[j + 1];
		}
	}
	col->length -= col->num_deleted;
	col->num_deleted = 0;
}

Table* table_for_column(Column* col) {
	for (size_t i = 0; i < current_db->tables_size; i++) {
		Table* table = &current_db->tables[i];
		for (size_t j = 0; j < table->columns_size; j++) {
			if (&table->columns[j] == col)
				return table;
		}
	}
	return NULL;
}

