#ifndef PARSE_H__
#define PARSE_H__
#include "cs165_api.h"
#include "message.h"
#include "client_context.h"

DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

DbOperator* parse_create(char* create_arguments, message* send_message);

DbOperator* parse_create_db(char* create_db_arguments, message* send_message);

DbOperator* parse_create_tbl(char* create_tbl_arguments, message* send_message);

DbOperator* parse_create_col(char* create_col_arguments, message* send_message);

DbOperator* parse_create_idx(char* create_idx_arguments, message* send_message);

DbOperator* parse_insert(char* insert_arguments, message* send_message);

DbOperator* parse_select(char* select_arguments, ClientContext* context, message* send_message);

DbOperator* parse_fetch(char* fetch_arguments, message* send_message);

DbOperator* parse_join(char* join_arguments, message* send_message);

DbOperator* parse_print(char* print_arguments, message* send_message);

DbOperator* parse_unary_aggregate(char* unary_agg_arguments, message* send_message);

DbOperator* parse_binary_aggregate(char* binary_agg_arguments, message* send_message);

DbOperator* parse_shutdown(message* send_message);

#endif
