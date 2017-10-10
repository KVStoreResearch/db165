#ifndef DB_OPERATORS_H__
#define DB_OPERATORS_H__
#include "cs165_api.h"

char* execute_create(DbOperator* query);

char* execute_create_db(DbOperator* query);

char* execute_create_tbl(DbOperator* query);

char* execute_create_col(DbOperator* query);

char* execute_open(DbOperator* query);

char* execute_insert(DbOperator* query);

char* execute_select(DbOperator* query);

char* execute_fetch(DbOperator* query);

char* execute_print(DbOperator* query);

char* execute_shutdown();
#endif
