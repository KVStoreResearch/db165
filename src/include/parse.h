#ifndef PARSE_H__
#define PARSE_H__
#include "cs165_api.h"
#include "message.h"
#include "client_context.h"

DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

DbOperator* parse_create(char* query_command, message* send_message);

DbOperator* parse_create_db(char* query_command, message* send_message);

DbOperator* parse_create_tbl(char* query_command, message* send_message);

DbOperator* parse_create_col(char* query_command, message* send_message);

DbOperator* parse_load(char* query_command, message* send_message);

DbOperator* parse_insert(char* query_command, message* send_message);

DbOperator* parse_select(char* query_command, message* send_message);

DbOperator* parse_fetch(char* query_command, message* send_message);

DbOperator* parse_shutdown(char* query_command, message* send_message);
#endif
