/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

typedef enum mode {
	DEFAULT,
	LOAD
} mode;

static mode current_mode = DEFAULT;

/*
 * create_client_context()
 * Creates client context and returns pointer to it
 */
ClientContext* create_client_context() {
	ClientContext* context = (ClientContext*) malloc(sizeof(ClientContext));
	if (!context) {
		log_err("Could not create client context.\n");
		exit(1);
	}
	context->chandles_in_use = 0;
	context->chandle_slots = MAX_NUM_HANDLES;
	context->chandle_table = (GeneralizedColumnHandle*) malloc(sizeof(GeneralizedColumnHandle)
			* context->chandle_slots);
}

int send_result(int client_socket, message send_message, char* result) {
	int total_length = strlen(result); 
	int length_sent = 0;

	while (length_sent != total_length) {
		send_message.length = total_length - length_sent > DEFAULT_RESULT_BUFFER_LENGTH
			? DEFAULT_RESULT_BUFFER_LENGTH : total_length - length_sent;
		send_message.status = total_length - length_sent > DEFAULT_RESULT_BUFFER_LENGTH
			? OK_WAIT_FOR_RESPONSE : OK_DONE;

		// Send status of the received message (OK, UNKNOWN_QUERY, etc)
		if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
			log_err("Failed to send message.");
			exit(1);
		}

		// Send response of request
		if (send(client_socket, result + length_sent, send_message.length, 0) == -1) {
			log_err("Failed to send message.");
			exit(1);
		}

		length_sent += send_message.length;
	}
}

int handle_client_default(int client_socket, ClientContext* client_context) {
	message recv_message;
	int length = recv(client_socket, &recv_message, sizeof(message), 0);
	if (length < 0) {
		log_err("Client connection closed!\n");
		exit(1);
	} else if (length == 0) {
		return 1;
	}

	char recv_buffer[recv_message.length + 1];
	length = recv(client_socket, recv_buffer, recv_message.length,0);
	recv_message.payload.text = recv_buffer;
	recv_message.payload.text[recv_message.length] = '\0';
	
	message send_message;
	char* result = NULL;
	if (strncmp(recv_message.payload.text, BEGIN_LOAD_MESSAGE, 
				strlen(BEGIN_LOAD_MESSAGE)) == 0) {
		current_mode = LOAD;
		result = "Load message received!";
		send_message.status = OK_BEGIN_LOAD;
	} else {
		// Parse command
		DbOperator* query = parse_command(recv_message.payload.text, &send_message, 
				client_socket, client_context);
		// Handle request
		result = execute_db_operator(query);
		db_operator_free(query);
	}
	send_result(client_socket, send_message, result);
	return 0;
}

int handle_client_load(int client_socket, ClientContext* context) {
	message recv_message, send_message;
	int length_received = recv(client_socket, &recv_message, sizeof(message), 0);
	if (length_received < 0) {
		log_err("Client connection closed!\n");
		exit(1);
	} else if (length_received == 0) {
		return -1;
	} 

	char* header_line = malloc(recv_message.length);
	length_received = recv(client_socket, header_line, recv_message.length, 0);
	if (length_received < 0) {
		log_err("Client connection closed!\n");
		exit(1);
	} else if (length_received == 0) {
		return -1;
	} 

	send_message.status = OK_WAIT_FOR_RESPONSE;
	send_message.payload.text = "Beginning load...\n";
	send_message.length = strlen(send_message.payload.text);
	if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
		log_err("Could not send client acknowledgment of load start.\n");
		return -1;
	}
	if (send(client_socket, send_message.payload.text, send_message.length, 0) == -1) {
		log_err("Could not send client acknowledgment payload of load start.\n");
		return -1;
	}

	int has_more = 1;
	int total_length_received = 0;
	int buf_capacity = DEFAULT_LOAD_BUFFER_LENGTH;
	int* buf = malloc(buf_capacity);
	if (!buf) {
		log_err("Could not allocate buffer for receiving data for load.\n");
	}

	while ((length_received = recv(client_socket, &recv_message, sizeof(message), 0)) > 0
			&& (recv_message.status == OK_WAIT_FOR_RESPONSE
			|| recv_message.status == OK_DONE)) {

		if (recv_message.length + total_length_received >= buf_capacity) {
			int* new_buf = realloc(buf, buf_capacity * 2);
			if (!new_buf) {
				log_err("Could not reallocate buffer for receiving data for load.\n");
			}
			buf = new_buf;
			buf_capacity *= 2;
		}

		length_received = recv(client_socket, (char*) buf + total_length_received, 
				recv_message.length, 0);
		if (length_received <= 0) {
			log_err("Error in receiving load data.\n");
			free(header_line);
			free(buf);
			send_message.status = EXECUTION_ERROR; 
		} else {
			send_message.status = OK_WAIT_FOR_RESPONSE;
		}
		
		if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
			log_err("Could not send confirmation of receipt of load data to client.\n");
			free(buf);
			free(header_line);
			return -1;
		}
		total_length_received += recv_message.length;

		if (recv_message.status == OK_DONE) 
			break;
	}

	if (length_received < 0 || recv_message.status != OK_DONE) {
		log_err("Client connection closed!\n");
		exit(1);
	} else if (length_received == 0) {
		return -1;
	} 

	send_message.status = OK_DONE;
	send_message.payload.text = "-- File loaded!\n";
	send_message.length = strlen(send_message.payload.text);
	if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
		log_err("Could not send confirmation of load completion.\n");
		free(buf);
		free(header_line);
		return -1;
	} 
	if (send(client_socket, send_message.payload.text, send_message.length, 0) == -1) {
		log_err("Could not send confirmation of load completion.\n");
		free(buf);
		free(header_line);
		return -1;
	} 

	Status load_status = load(header_line, buf, total_length_received/ sizeof(int));
	if (load_status.code != OK) {
		log_err("Error occured when loading the database.\n");
		return -1;
	}

	current_mode = DEFAULT;
	free(buf);
	free(header_line);
	return 0;
}

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
void handle_client(int client_socket) {
    int done = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // create the client context here
    ClientContext* client_context = create_client_context();

    do {
		switch (current_mode) {
			case LOAD:
				done = handle_client_load(client_socket, client_context);
				break;
			default:
				done = handle_client_default(client_socket, client_context);
				break;
		}
    } while (!done);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
	// startup db
	Status ret_status = db_startup();
	if (ret_status.code != OK) {
		log_err("Could not start up database.\n");
		log_err(ret_status.error_message);
	} 

    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
        log_err("L%d: Failed to accept a new connection.\n", __LINE__);
        exit(1);
    }

	// Handle client messages
    handle_client(client_socket);

    return 0;
}
