#include "cs165_api.h"
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

