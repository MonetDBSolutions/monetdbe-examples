#include <monetdbe.h>
#include <stdio.h>
#include <string.h>

int 
main(int argc, char **argv) 
{
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
	}

	if (monetdbe_query(db, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL, &rows_affected) != NULL) {
		fprintf(stderr, "Failed to query database\n");
	}

	if (monetdbe_query(db, "SELECT * FROM integers", &result, NULL) != NULL) {
		fprintf(stderr, "Failed to query database\n");
	}
	// print the names of the result
	for (size_t i = 0; i < result->ncols; i++) {
		monetdbe_result_fetch(result, &cols[i], i);
		printf("%s ", cols[i]->name);
	}

	printf("\r\n");

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

	char* csv_path = NULL;
	if (argc > 1)
		csv_path = argv[1];
	else
		csv_path = "/tmp/dummy.csv";
	char* err = NULL;

	char* head="COPY SELECT * FROM integers INTO '";
	char* tail = "' USING DELIMITERS ',' NULL AS ''";
	char sql[1000];
	strcpy(sql, head);
	strcat(sql, csv_path);
	strcat(sql, tail);

	if((err=monetdbe_query(db, sql, NULL, NULL)) != NULL) {
		printf("%s\r\n", err);
		return -1;
	}
	return 0;
}
    
