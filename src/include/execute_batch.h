#include "cs165_api.h"

int init_batch(BatchOperator* batch);

int handle_db_operator(DbOperator* op, BatchOperator* batch);

char* execute_db_batch();
