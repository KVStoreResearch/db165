#include "utils.h"

int main(int argc, char** argv) {
	char* result;
	result = itoa(123);
	printf("%s", result);

	result = itoa(-123);
	printf("%s", result);
	
	return 0;
}
