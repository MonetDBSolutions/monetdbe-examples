#include "monetdbe.h"
#include <stdio.h>

int main() {
	monetdbe_database db = NULL;
	monetdbe_result *result;
	monetdbe_column *cols[2];
	size_t rows_affected = 0;

	if (monetdbe_open(&db, NULL /* inmemory database */, NULL /* no options */)) {
		fprintf(stderr, "Failed to open database\n");
		return -1;
	}
	if (monetdbe_query(db, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL, NULL) != NULL) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (monetdbe_query(db, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL, &rows_affected) != NULL) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	printf("inserted %d rows\n", rows_affected);
	if (monetdbe_query(db, "SELECT * FROM integers", &result, NULL) != NULL) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	// print the names of the result
	for (size_t i = 0; i < result->ncols; i++) {
		monetdbe_result_fetch(result, &cols[i], i);
		printf("%s ", cols[i]->name);
	}
	printf("\n");
	// print the data of the result
	for (size_t row_idx = 0; row_idx < result->nrows; row_idx++) {
		for (size_t col_idx = 0; col_idx < result->ncols; col_idx++) {
			assert(cols[col_idx]->type == monetdbe_int32_t);
			monetdbe_column_int32_t *col = (monetdbe_column_int32_t *)cols[col_idx];
			int val = col->data[row_idx];
			if (val == col->null_value)
				printf("NULL ");
			else
				printf("%d ", val);
		}
		printf("\n");
	}
	// monetdbe_print_result(result);
cleanup:
	monetdbe_cleanup_result(db, result);
	monetdbe_close(&db);
}
