#include "cs165_api.h"

typedef struct JoinThreadArgs {
	Column* positions_a;
	Column* positions_b;
	Column* values_a;
	Column* values_b;
	Status* status;
	Column*** results;
} JoinThreadArgs;

Column** join(Column* positions_1, Column* positions_2, Column* values_1, Column* values_2,
		JoinType type, Status* status);
