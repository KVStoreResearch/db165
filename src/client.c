/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "client_load.h"
#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload.text = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
           break;
        } else if (strncmp(read_buffer, "--", 2) == 0) {
			continue;
		}

        // Only process input that is greater than 2 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
			if (strncmp(read_buffer, "load", 4) == 0) {
				char read_buffer_copy[DEFAULT_STDIN_BUFFER_SIZE];
				strncpy(read_buffer_copy, read_buffer, strlen(read_buffer) + 1);
				if (send_load_data(read_buffer_copy, client_socket) == -1) {
					log_err("Could not load data.");
					printf("-- Could not load data.");
				}
				continue;
			} 

            // Send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

			// Send the payload (query) to server
			if (send(client_socket, send_message.payload.text, send_message.length, 0) == -1) {
				log_err("Failed to send query payload.");
				exit(1);
			}


			// Always wait for server response (even if it is just an OK message)
			while ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0
					&& (recv_message.status == OK_WAIT_FOR_RESPONSE
					|| recv_message.status == OK_DONE)) {
				// Calculate number of bytes in response package
				int length = (int) recv_message.length;
				char payload[length + 1];

				// Receive the payload and print it out
				if ((len = recv(client_socket, payload, length, 0)) > 0) {
					payload[length] = '\0';
					printf("%s", payload);
				}

				if (recv_message.status == OK_DONE)  {
					printf("\n");
					break;
				}
			}

			if (len < 0) {
				log_err("Server closed connection\n");
				exit(1);
			} else if (recv_message.status != OK_DONE) {
				log_err("Server could not complete request\n");
				exit(1);
			}
        }
    }
    close(client_socket);
    return 0;
}

