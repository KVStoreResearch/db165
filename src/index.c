#include <string.h>

#include "btree.h"
#include "cs165_api.h"
#include "index.h"
#include "utils.h"

Status construct_sorted_index(Column* column, Table* table, bool clustered) {
	Status ret_status;

	int** idx_data = NULL;
	if (clustered) {
		sort(column->data, column->length, NULL, table);
		idx_data = malloc(sizeof *idx_data * table->columns_size);
		for (size_t i = 0; i < table->columns_size; i++) {
			idx_data[i] = table->columns[i].data;
			if (column != &table->columns[i] && table->columns[i].index)
				construct_index(&table->columns[i], table);
		}
		column->index->positions = NULL; // no need for positions array with clustered index
	} else {
		int* sorted_copy = malloc(column->length * sizeof *sorted_copy);
		if (!sorted_copy) {
			ret_status.code = ERROR;
			log_err("Could not allocate memory for sorted copy of column %s.\n", column->name);
			return ret_status;
		}

		if (!memcpy(sorted_copy, column->data, column->length * sizeof *sorted_copy)) {
			ret_status.code = ERROR;
			log_err("Could not copy data from column %s for sorted index.\n");
			return ret_status;
		}

		int* positions = malloc(column->length * sizeof *positions);
		if (!positions) {
			ret_status.code = ERROR;
			log_err("Could not allocate memory for positions array for sorted index on column %s.\n"
					, column->name);
			return ret_status;
		}

		for (int i = 0; i < column->length; i++)
			positions[i] = i;

		sort(sorted_copy, column->length, positions, NULL);

		idx_data = malloc(sizeof *idx_data);	
		*idx_data = sorted_copy;
		column->index->positions = positions;
	}
	column->index->data = idx_data;
	
	ret_status.code = OK;
	log_info("Successfully constructed sorted index on column %s in table %s.\n", column->name
			, table->name);
	return ret_status;
}

Status construct_btree_index(Column* column) {
	Status ret_status;
	
	Btree* index = alloc_btree();

	for (size_t i = 0; i < column->length; i += DEFAULT_BTREE_NODE_CAPACITY) {
		insert(&column->index->data[0][i], index);
	}

	column->index->tree = index;
	ret_status.code = OK;
	return ret_status;
}

Status construct_index(Column* column, Table* table) {
	Status ret_status;
	ret_status = construct_sorted_index(column, table, column->clustered);
	if (ret_status.code != OK) {
		log_err("Could not construct sorted index on column %s\n", column->name);
		return ret_status;
	}

	if (column->index->type == BTREE) {
		ret_status = construct_btree_index(column);
		if (ret_status.code != OK) {
			log_err("Could not construct btree index on column %s\n", column->name);
			return ret_status;
		}
	}

	column->stale_index = false;
	ret_status.code = OK;
	log_info("CONSTRUCTED INDEX ON COLUMN %s\n", column->name);
	return ret_status;
}

/*
 *
 * Sorting used for creating sorted indices
 * Mergesort implementation from GeeksForGeeks
 * http://www.geeksforgeeks.org/merge-sort/
 */

void merge(int* arr, int* p, int** t, int num_cols, int l, int m, int r) {
	int i, j, k;
	int n1 = m - l + 1;
	int n2 = r - m;

	int L[n1], R[n2];
	int P_L[n1], P_R[n2];
	int** T_L = NULL; 
	int** T_R = NULL;

	if (t) {
		T_L = malloc(sizeof *T_L * num_cols);
		T_R = malloc(sizeof *T_R * num_cols);
		for (int z = 0; z < num_cols; z++) {
			T_L[z] = malloc(sizeof *T_L[z] * n1);
			T_R[z] = malloc(sizeof *T_R[z] * n2);
		}
	} 

	for (i = 0; i < n1; i++) {
		L[i] = arr[l + i];
		if (p)
			P_L[i] = p[l + i];
		if (t)
			for (int z = 0; z < num_cols; z++)
				T_L[z][i] = t[z][l + i];
	}
	for (j = 0; j < n2; j++)  {
		R[j] = arr[m + 1 + j];
		if (p)
			P_R[j] = p[m + 1 + j];
		if (t)
			for (int z = 0; z < num_cols; z++)
				T_R[z][j] = t[z][m + 1 + j];
	}

	i = 0; 
	j = 0;
	k = l;

	while (i < n1 && j < n2) {
		if (L[i] <= R[j]) {
			arr[k] = L[i];
			if (p)
				p[k] = P_L[i];
			if (t)
				for (int z = 0; z < num_cols; z++) 
					t[z][k] = T_L[z][i];
			i++;
		} else {
			arr[k] = R[j];
			if (p)
				p[k] = P_R[j];
			if (t)
				for (int z = 0; z < num_cols; z++) 
					t[z][k] = T_R[z][j];
			j++;
		}
		k++;
	}

	while (i < n1) {
		arr[k] = L[i];
		if (p)
			p[k] = P_L[i];
		if (t)
			for (int z = 0; z < num_cols; z++) 
				t[z][k] = T_L[z][i];
		i++;
		k++;
	}

	while (j < n2) {
		arr[k] = R[j];
		if (p)
			p[k] = P_R[j];
		if (t)
			for (int z = 0; z < num_cols; z++) 
				t[z][k] = T_R[z][j];
		j++;
		k++;
	}

	if (t) {
		for (int z = 0; z < num_cols; z++) {
			free(T_L[z]);
			free(T_R[z]);
		}
		free(T_L);
		free(T_R);
	}
}

void mergeSort(int* arr, int* p, int** t, int num_cols, int l, int r) {
	if (l < r) {
		int m = l + (r - l) / 2;

		mergeSort(arr, p, t, num_cols, l, m);
		mergeSort(arr, p, t, num_cols, m+1, r);
		merge(arr, p, t, num_cols, l, m, r);
	}
}
// returns vector of positions corresponding to sorted order of arr
int* sort(int* arr, int arr_len, int* positions, Table* table) {
	int** t = NULL;
	if (table)  {
		t = malloc(sizeof *t * table->columns_size);
		for (int i = 0; i < table->columns_size; i++) {
			t[i] = table->columns[i].data;
		}
	}
	
	mergeSort(arr, positions, t, table ? table->columns_size : 0, 0, arr_len -1);
	return positions;
}

