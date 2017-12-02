#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "btree.h"
#include "cs165_api.h"

Btree* alloc_btree() {
	Btree* tree = malloc(sizeof *tree);
	Node* root = malloc(sizeof *root);
	root->is_leaf = true;
	root->length = 0;
	tree->root = root;
	
	return tree;
}

// Inserts a fence into correct position within a node
void insert_internal(int val, Node* node, Node* less, Node* greater) {
	int ix = 0;
	while (ix < node->length && val > node->vals[ix])
		ix++;

	for (int i = node->length; i >= ix; i--)
		node->vals[i] = node->vals[i - 1];

	for(int i = node->length + 1; i > ix; i--)
		node->children[i] = node->children[i-1];

	node->vals[ix] = val;
	node->length++;
	node->children[ix] = less;
	node->children[ix+1] = greater;

	return;
}

// Splits next into two nodes, and puts respective fence in current
// returns parent node of two split nodes
Node* split(Node* current, Node* next) {
	Node* new = malloc(sizeof *new);	
	new->length = 0;
	new->is_leaf = next->is_leaf;

	int split = next->length / 2;
	int num_nodes_to_move = next->length - (split + 1);
	if (!memcpy(new->vals, &next->vals[split + 1], sizeof *new->vals * num_nodes_to_move)) {
		return NULL;
	}
	new->length += num_nodes_to_move;
	if (!memcpy(new->children, &next->children[split + 1], sizeof *new->children * (num_nodes_to_move+1))) {
		return NULL;
	}

	if (!current) {
		current = malloc(sizeof *current);
		current->is_leaf = false;
		current->length = 0;
	}

	insert_internal(next->vals[split], current, next, new);
	for (int i = split; i < next->length; i++) {
		next->vals[i] = 0;
		next->children[i+1] = NULL;
	}
	next->length -= num_nodes_to_move + 1;

	next->length = split; // effectively get rid of fences no longer needed here
	return current; 	
}

void insert_leaf(int* key, Node* current) {
	int ix = 0;
	while (ix < current->length && *key > current->vals[ix])
		ix++;

	for (int i = current->length; i > ix; i--)
		current->vals[i] = current->vals[i-1];

	current->vals[ix] = *key;
	current->children[ix] = key;
	current->length++;
}

void insert(int* key, Btree* tree) {
	if (tree->root->length == DEFAULT_BTREE_NODE_CAPACITY) {
		tree->root = split(NULL, tree->root);
	}

	Node* current = tree->root;
	Node* next = NULL;

	while (!current->is_leaf) { // find node to insert val into
		for (int i = 0; i <= current->length; i++) {
			if ((i == 0 && *key < current->vals[i])
					|| (i > 0 && i == current->length && current->vals[i-1] <= *key)
					|| (i > 0 && i < current->length &&
						current->vals[i-1] <= *key && *key < current->vals[i])) {
				next = (Node*) current->children[i];
				if (next->length == DEFAULT_BTREE_NODE_CAPACITY) {
					current = split(current, next);
					continue;
				}
				current = next;
				break;
			}
		}
	}

	insert_leaf(key, current);
	return;
}

void inOrderTraversal(Node* node) {
	for (int i = 0; i < node->length; i++) {
		if (!node->is_leaf)
			inOrderTraversal(node->children[i]);
		printf("%d,", node->vals[i]);
	}
	if (!node->is_leaf)
		inOrderTraversal(node->children[node->length]);
	return;
}

void testN(int n, Btree* tree) {
	int* sorted = malloc(sizeof *sorted * n);
	for (int i = 0; i < n; i++) {
		sorted[i] = i;
		insert(&sorted[i], tree);
		if (i % 20 == 0)
			printf("\n");
	}

	inOrderTraversal(tree->root);
}
