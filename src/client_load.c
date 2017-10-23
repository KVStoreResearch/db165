#define _BSD_SOURCE
#include <errno.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include "client_load.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_BUFFER_SIZE 1024
#define DEFAULT_PARTITION_LENGTH 8192

int send_text_data(int client_socket, char* payload_text, message_status expected_status) {
	message send_message, recv_message;

	send_message.payload.text = payload_text;
	send_message.length = strlen(send_message.payload.text);

	if (send(client_socket, &send_message, sizeof(message), 0) == -1) {
		log_err("Could not send data to server.\n");
		printf("Could not send data to server.\n");
		return -1;
	}
	if (send(client_socket, send_message.payload.text, send_message.length, 0) == -1) {
		log_err("Could not send data to server.\n");
		printf("Could not send data to server.\n");
		return -1;
	}

	int len;
	if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
		if ((int) len > 0 && recv_message.status != expected_status) {
			log_err("Error occurred when loading/sending file to server.\n");
			printf("Error occurred when loading/sending file to server.\n");
			return -1;
		}
		// Calculate number of bytes in response package
		int num_bytes = (int) recv_message.length;
		char payload[num_bytes + 1];

		// Receive the payload and print it out
		if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
			payload[num_bytes] = '\0';
			printf("%s\n", payload);
		}
	}
	else {
		if (len < 0) {
			log_err("Failed to receive message.");
			printf("Failed to receive message.");
			return -1;
		}
		else {
			log_info("Server closed connection\n");
			exit(1);
		}
	}

	return 0;
}

int send_load_data(char* read_buffer, int client_socket)  {
	printf("-- Starting to send load data\n");
	char* filename = extract_load_filename(read_buffer);
	FILE* fp = fopen(filename, "r");
	if (!fp) { 
		log_err("Client could not open file %s for loading.\n", filename);
		printf("-- Client could not open file %s for loading.\n", filename);
		return -1;
	}

	struct stat file_stat;
	if (stat(filename, &file_stat) == -1) {
		log_err("Could not acquire file stats.\n Error: %s", strerror(errno));
		printf("-- Could not acquire file stats.\n Error: %s", strerror(errno));
		return -1;
	}

	int* buf = malloc(file_stat.st_size);	
	if (!buf) { 
		log_err("Client could not allocate buffer for reading from loaded file.\n");
		printf("--Client could not allocate buffer for reading from loaded file.\n");
		return -1;
	}

	char* text_buf = malloc(DEFAULT_BUFFER_SIZE);
	if (!text_buf) { 
		log_err("Client could not allocate buffer for reading header from loaded file.\n");
		printf("-- Client could not allocate buffer for reading header from loaded file.\n");
		return -1;
	}
	if (!fgets(text_buf, DEFAULT_BUFFER_SIZE, fp)) {
		log_err("Could not read header line of file.\n");	
		printf("-- Could not read header line of file.\n");	
		return -1;
	}
	char* header_line = strdup(text_buf);
	free(text_buf);

	int total_length = read_data(fp, buf);
	if (total_length == -1) {
		log_err("Could not read data in from file.\n");
		printf("-- Could not read data in from file.\n");
		return -1;
	}

	if (send_text_data(client_socket, BEGIN_LOAD_MESSAGE, OK_BEGIN_LOAD) == -1) {
		log_err("Error occured when sending load initilization message to server.\n");
		printf("-- Error occured when sending load initilization message to server.\n");
		return -1;
	}

	if (send_text_data(client_socket, header_line, OK_WAIT_FOR_RESPONSE) == -1) {
		log_err("Error occured when sending load initilization message to server.\n");
		printf("-- Error occured when sending load initilization message to server.\n");
		return -1;
	}


	message send_message, recv_message;
	int length_sent, length_received;
	length_sent = length_received = 0;
	while (length_sent != total_length) {
		send_message.length = total_length - length_sent > DEFAULT_LOAD_BUFFER_LENGTH
			? DEFAULT_LOAD_BUFFER_LENGTH : total_length - length_sent;
		send_message.status = total_length - length_sent > DEFAULT_LOAD_BUFFER_LENGTH 
			? OK_WAIT_FOR_RESPONSE : OK_DONE;
		if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
			log_err("Failed to send data for loading.\n");
			printf("-- Failed to send data for loading.\n");
			return -1;
		}

		if (send(client_socket, (char*) buf + length_sent, send_message.length, 0) == -1) {
			log_err("Failed to send data for loading.\n");
			printf("Failed to send data for loading.\n");
			return -1;
		}

		length_sent += send_message.length;

		// Always wait for server response (even if it is just an OK message)
		if ((length_received = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
			if (recv_message.status != OK_WAIT_FOR_RESPONSE) {
				log_err("Error occurred when loading/sending file to server.\n");
				printf("Error occurred when loading/sending file to server.\n");
				return -1;
			}
		}
		else {
			if (length_received < 0) {
				log_err("Failed to receive message.");
				printf("Failed to receive message.");
				return -1;
			}
		}
	}

	if (recv(client_socket, &recv_message, sizeof(message), 0) == -1) {
		log_err("Could not receive confirmation of load completion.\n");
		printf("Could not receive confirmation of load completion.\n");
		return -1;
	}

	if (recv_message.status != OK_DONE) {
		log_err("Error occurred in loading file.\n");
		printf("Error occurred in loading file.\n");
		return -1;
	} else {
		char* payload = malloc(recv_message.length + 1);
		if (recv(client_socket, payload, recv_message.length, 0) == -1) {
			log_err("Could not receive confirmation of load completion payload.\n");
			printf("Could not receive confirmation of load completion payload.\n");
			return -1;
		}
		printf("%s", payload);
		free(payload);
	}

	return 0;
}

int read_data(FILE* fp, int* buf) {
	char read_buf[DEFAULT_BUFFER_SIZE];
	long data_read = 0;
	while (fgets(read_buf, DEFAULT_BUFFER_SIZE, fp)) {
		char* line_copy = strdup(read_buf);
		char* token;
		while ((token = strsep(&line_copy, ",")) != NULL) {
			buf[data_read] = atoi(token);
			data_read++;
		}		
	}
	return data_read * sizeof *buf;
}

char* extract_load_filename(char* buffer) {
	trim_newline(buffer);
	printf("-- Buffer to extract filename from : %s\n", buffer);
	if (strncmp(buffer, "load(", 5) != 0) {
		printf("-- First 5 chars were not load(\n");
		return NULL;
	}
	buffer += 5;
	trim_quotes(buffer);
	trim_whitespace(buffer);

	int last_char_ix = strlen(buffer) - 1;
	if (buffer[last_char_ix] != ')') {
		printf("-- Last char was not )\n");
		return NULL;
	}
	buffer[last_char_ix] = '\0';
	
	return buffer;
}
