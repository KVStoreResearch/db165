#define _BSD_SOURCE
/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#include <limits.h>
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>

#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/
char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    cs165_log(stdout, "QUERY: %s\n", query_command);

    DbOperator *dbo = NULL;

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    send_message->status = OK_DONE;
    query_command = trim_whitespace(query_command);

    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command, send_message);
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
		add_handle(context, handle, false);
		query_command += 6;
		dbo = parse_select(query_command, send_message);
		if (dbo)
			dbo->operator_fields.select_operator.result_handle = handle;
	} else if (strncmp(query_command, "fetch", 5) == 0) {
		add_handle(context, handle, false);
		query_command += 5;
		dbo = parse_fetch(query_command, send_message);
		if (dbo)
			dbo->operator_fields.fetch_operator.result_handle = handle;
	} else if (strncmp(query_command, "print", 5) == 0) {
		query_command += 5;
		dbo = parse_print(query_command, send_message);
	} else if (strncmp(query_command, "shutdown", 8) == 0) {
		dbo = parse_shutdown(send_message);
	} else if (strncmp(query_command, "avg", 3) == 0
			|| strncmp(query_command, "sum", 3)
			|| strncmp(query_command, "min", 3)
			|| strncmp(query_command, "max", 3)) {
		add_handle(context, handle, false);
		OperatorType op_type;
		if (strncmp(query_command, "avg", 3) == 0)
			op_type = AVERAGE;
		if (strncmp(query_command, "sum", 3) == 0)
			op_type = SUM;
		if (strncmp(query_command, "max", 3) == 0)
			op_type = MAX;
		if (strncmp(query_command, "min", 3) == 0)
			op_type = MIN;

		query_command += 3;

		dbo = parse_unary_aggregate(query_command, send_message);
		dbo->type = op_type;
		if (dbo)
			dbo->operator_fields.unary_aggregate_operator.result_handle = handle;
	} else if (strncmp(query_command, "add", 3) == 0
			|| strncmp(query_command, "sub", 3) == 0) {
	} 

    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments, message* send_message) {
    // Since strsep destroys input, we create a copy of our input. 
    char *tokenizer_copy, *to_free, *token;
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    strcpy(tokenizer_copy, create_arguments);

    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                return parse_create_db(tokenizer_copy, send_message);
            } else if (strcmp(token, "tbl") == 0) {
                return parse_create_tbl(tokenizer_copy, send_message);
			} else if (strcmp(token, "col") == 0) {
				return parse_create_col(tokenizer_copy, send_message);
            } else {
                send_message->status = UNKNOWN_COMMAND;
            }
        }
    } else {
		send_message->status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return NULL;
}


/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/
DbOperator* parse_create_db(char* create_db_arguments, message* send_message) {
    char *token;
    token = strsep(&create_db_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
		send_message->status = INCORRECT_FORMAT;
        return NULL;                    
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
			send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_db_arguments, ",");
        if (token != NULL) {
			send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

		DbOperator* create_db_operator = (DbOperator*) malloc(sizeof(DbOperator));
		create_db_operator->type = CREATE;
		create_db_operator->operator_fields.create_operator.type = DB;
		create_db_operator->operator_fields.create_operator.name = db_name;

		return create_db_operator;
    }
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/
DbOperator* parse_create_tbl(char* create_tbl_arguments, message* send_message) {
    char** create_arguments_index = &create_tbl_arguments;
    char* table_name = next_token(create_arguments_index, &send_message->status);
    char* db_name = next_token(create_arguments_index, &send_message->status);
    char* col_cnt = next_token(create_arguments_index, &send_message->status);

    // not enough arguments
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }

    // get the table name free of quotation marks
    table_name = trim_quotes(table_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
		send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';

    // check that the database argument is the current active database
    if (strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name\n");
		send_message->status = QUERY_UNSUPPORTED;
        return NULL;
    }

    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
		send_message->status = INCORRECT_FORMAT;
        return NULL;
    }

	DbOperator* create_tbl_operator = (DbOperator*) malloc(sizeof(DbOperator));
	create_tbl_operator->type = CREATE;
	create_tbl_operator->operator_fields.create_operator.type = TBL;
	create_tbl_operator->operator_fields.create_operator.name = table_name;
	create_tbl_operator->operator_fields.create_operator.column_count = column_cnt;

	return create_tbl_operator;
}

/**
 * This method takes in a string representing the arguments to create a column.
 * It parses those arguments, checks that they are valid, and creates a column.
 **/
DbOperator* parse_create_col(char* create_col_arguments, message* send_message) {
	char** create_arguments_index = &create_col_arguments;
    char* col_name = next_token(create_arguments_index, &send_message->status);
    char* db_and_table_name = next_token(create_arguments_index, &send_message->status);

    // Not enough arguments
    if (send_message->status == INCORRECT_FORMAT) {
        return NULL;
    }
	// Get the table name free of quotation marks
    col_name = trim_quotes(col_name);

    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_and_table_name) - 1;
    if (db_and_table_name[last_char] != ')') {
		send_message->status = INCORRECT_FORMAT;
        return NULL;
    }
    // replace the ')' with a null terminating character. 
	db_and_table_name[last_char] = '\0';

	// Find table to pass to create_col
	Table* current_table = lookup_table(db_and_table_name);
	if (!current_table) {
		send_message->status = INCORRECT_FORMAT;
		return NULL;
	}

	DbOperator* create_col_operator = (DbOperator*) malloc(sizeof(DbOperator));
	create_col_operator ->type = CREATE;
	create_col_operator->operator_fields.create_operator.type = COL;
	create_col_operator->operator_fields.create_operator.name = col_name;
	create_col_operator->operator_fields.create_operator.table = current_table;

    return create_col_operator;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/
DbOperator* parse_insert(char* insert_arguments, message* send_message) {
    char *tokenizer_copy, *to_free, *token;
    tokenizer_copy = to_free = malloc((strlen(insert_arguments)+1) * sizeof(char));
    strcpy(tokenizer_copy, insert_arguments);

    unsigned int columns_inserted = 0;
    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        char** command_index = &tokenizer_copy;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->columns_size);

        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->columns_size) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 

		free(to_free);
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/*
 * parse_select
 */
DbOperator* parse_select(char* select_arguments, message* send_message) {
    char *tokenizer_copy, *to_free, *token;
    tokenizer_copy = to_free = malloc(strlen(select_arguments)+1);
    strcpy(tokenizer_copy, select_arguments);

    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
		send_message->status = UNKNOWN_COMMAND;
		return NULL;
    }

	tokenizer_copy++;
	char** command_index = &tokenizer_copy;

	char* column_name = next_token(command_index, &send_message->status);
	if (send_message->status == INCORRECT_FORMAT) {
		return NULL;
	}

	// lookup the table and make sure it exists. 
	Column* select_column = lookup_column(column_name);
	if (!select_column) {
		send_message->status = OBJECT_NOT_FOUND;
		return NULL;
	}
	
	// parse upper and lower bounds
	token = strsep(command_index, ",");
	int low = INT_MIN;
	if (strncmp(token, "null", 4) != 0) {
		low = atoi(token);
	}
	token = strsep(command_index, ",");
	int high = INT_MAX;
	if (strncmp(token, "null", 4) != 0) {
		high = atoi(token);
	}

	// make select operator. 
	DbOperator* dbo = malloc(sizeof(DbOperator));
	dbo->type = SELECT;
	dbo->operator_fields.select_operator.column = select_column;
	dbo->operator_fields.select_operator.low = low;
	dbo->operator_fields.select_operator.high = high;
	dbo->operator_fields.select_operator.positions = NULL;

	//TODO parse other arguments
	return dbo;
}

/* 
 * parse fetch
 */
DbOperator* parse_fetch(char* fetch_arguments, message* send_message) {
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc(strlen(fetch_arguments)+1);
    strcpy(tokenizer_copy, fetch_arguments);

    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
		send_message->status = UNKNOWN_COMMAND;
		return NULL;
    }
	tokenizer_copy++;
	char** command_index = &tokenizer_copy;

	char* column_name = next_token(command_index, &send_message->status);
	if (send_message->status == INCORRECT_FORMAT) {
		return NULL;
	}

	// lookup the table and make sure it exists. 
	Column* fetch_column = lookup_column(column_name);
	if (!fetch_column) {
		send_message->status = OBJECT_NOT_FOUND;
		return NULL;
	}

	char* handle = next_token(command_index, &send_message->status);
	int last_char = strlen(handle) - 1;
	if (last_char < 0 || handle[last_char] != ')') {
		send_message->status = INCORRECT_FORMAT;
		return NULL;
	}
	// replace final ')' with null-termination character.
	handle[last_char] = '\0';
	if (send_message->status == INCORRECT_FORMAT) {
		return NULL;
	}

	// construct fetch operator. 
	DbOperator* dbo = malloc(sizeof(DbOperator));
	dbo->type = FETCH;
	dbo->operator_fields.fetch_operator.column = fetch_column;
	dbo->operator_fields.fetch_operator.positions_handle = handle;

	return dbo;
}

DbOperator* parse_print(char* print_arguments, message* send_message) {
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(print_arguments)+1) * sizeof(char));
    strcpy(tokenizer_copy, print_arguments);

    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
		send_message->status = UNKNOWN_COMMAND;
		return NULL;
    }
	tokenizer_copy++;
	char** command_index = &tokenizer_copy;

	char* handle = next_token(command_index, &send_message->status);
	if (send_message->status == INCORRECT_FORMAT) {
		return NULL;
	}

	int last_char = strlen(handle) - 1;
	if (last_char < 0 || handle[last_char] != ')') {
		send_message->status = INCORRECT_FORMAT;
		return NULL;
	}
	handle[last_char] = '\0';

	DbOperator* dbo = (DbOperator*) malloc(sizeof(DbOperator));	
	dbo->type = PRINT;
	dbo->operator_fields.print_operator.handle = handle;
	send_message->status = OK_DONE;

	return dbo;
}

DbOperator* parse_unary_aggregate(char* unary_agg_arguments, message* send_message) {
    char *tokenizer_copy, *to_free;
    tokenizer_copy = to_free = malloc((strlen(unary_agg_arguments)+1) * sizeof(char));
    strcpy(tokenizer_copy, unary_agg_arguments);

    // check for leading '('
    if (strncmp(tokenizer_copy, "(", 1) != 0) {
		send_message->status = UNKNOWN_COMMAND;
		return NULL;
    }
	tokenizer_copy++;
	char** command_index = &tokenizer_copy;

	char* handle = next_token(command_index, &send_message->status);
	if (send_message->status == INCORRECT_FORMAT) {
		return NULL;
	}

	int last_char = strlen(handle) - 1;
	if (last_char < 0 || handle[last_char] != ')') {
		send_message->status = INCORRECT_FORMAT;
		return NULL;
	}
	handle[last_char] = '\0';

	DbOperator* dbo = (DbOperator*) malloc(sizeof(DbOperator));	
	dbo->operator_fields.unary_aggregate_operator.handle = handle;
	send_message->status = OK_DONE;
	
	return dbo;
}
DbOperator* parse_shutdown(message* send_message) {
	DbOperator* dbo = malloc(sizeof(DbOperator));
	dbo->type = SHUTDOWN;
	send_message->status = OK;
	return dbo;
}
