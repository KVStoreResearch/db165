#include "cs165_api.h"

int send_load_data(char* read_buffer, int sock_fd);

int read_data(FILE* fp, int* buf);

char* extract_load_filename(char* buffer);
