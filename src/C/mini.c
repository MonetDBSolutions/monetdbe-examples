#include "monetdbe.h"
#include <stdio.h>

int main() {
	monetdbe_database db = NULL;
	monetdbe_result result;

	if (monetdbe_open(&db) ) {
		fprintf(stderr, "Failed to open database\n");
		return -1;
	}
	if (monetdbe_query(con, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (monetdbe_query(con, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	if (monetdbe_query(con, "SELECT * FROM integers", &result) == DuckDBError) {
		fprintf(stderr, "Failed to query database\n");
		goto cleanup;
	}
	// print the names of the result
	for (size_t i = 0; i < result.column_count; i++) {
		printf("%s ", result.columns[i].name);
	}
	printf("\n");
	// print the data of the result
	for (size_t row_idx = 0; row_idx < result.row_count; row_idx++) {
		for (size_t col_idx = 0; col_idx < result.column_count; col_idx++) {
			char *val = monetdbe_value_varchar(&result, col_idx, row_idx);
			printf("%s ", val);
			free(val);
		}
		printf("\n");
	}
	// monetdbe_print_result(result);
cleanup:
	monetdbe_destroy_result(&result);
	if( monetdbe_close(&db))
		printf("Could not close the database");;
}
