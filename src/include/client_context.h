#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

bool table_exists(char* db_name, char* table_name);

bool column_exists(char* db_name, char* table_name, char* column_name);

Table* lookup_table(char* name);

Table* lookup_table_for_column(char* name);

Column* lookup_column(char* name);

GeneralizedColumnHandle* lookup_client_handle(ClientContext* context, char* handle);

int add_handle(ClientContext* context, char* handle, bool result);

int remove_handle(ClientContext* context, char* handle);

#endif
