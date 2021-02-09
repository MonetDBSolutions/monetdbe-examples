#include <monetdbe.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h> 
#include <pthread.h> 

static monetdbe_database db = NULL;

int open_database();
int create_tables(void);
void* write_tables(void* args);
void* read_tables(void* args);
void print_result();
int copy_into(char* csv_path);

int main(int argc, char **argv) {

    open_database();

    create_tables();
    void *result;

    pthread_t thread_id;
    pthread_create(&thread_id, NULL, write_tables, NULL); 
    pthread_join(thread_id, NULL); 

    pthread_t tid;
    pthread_create(&tid, NULL, read_tables, NULL); 
    pthread_join(tid, &result); 


    print_result(result);

    if(argc < 2) {
        fprintf(stderr, "csv file not specified");
        return -1;
    }

    char* csv_path = argv[1];
    return copy_into(csv_path);
}
    
int open_database() {
	if (monetdbe_open(&db, NULL /* inmemory database */, NULL /* no options */)) {
		fprintf(stderr, "Failed to open database\n");
		return -1;
	}

    return 0;
}

int create_tables(void) {
	if (monetdbe_query(db, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL, NULL) != NULL) {
		fprintf(stderr, "Failed to query database\n");
        return -1;
	}

    return 0;
}

void* write_tables(void* args) {
	size_t rows_affected = 0;
	if (monetdbe_query(db, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL, &rows_affected) != NULL) {
		fprintf(stderr, "Failed to query database\n");
	}
}

void* read_tables(void* args) {
	monetdbe_result *result;
	if (monetdbe_query(db, "SELECT * FROM integers", &result, NULL) != NULL) {
		fprintf(stderr, "Failed to query database\n");
        return NULL;
	}

    return (void *)result;
}
 
void print_result(monetdbe_result* result) {
	monetdbe_column *cols[2];

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
}

int copy_into(char* csv_path) {
    char* err = NULL;

    char* head="COPY SELECT * FROM integers INTO '";
    char* tail = "' USING DELIMITERS ',' NULL AS ''";
    char sql[1000];
    strcpy(sql, head);
    strcat(sql, csv_path);
    strcat(sql, tail);
    printf("%s\n", sql);
    if((err=monetdbe_query(db, sql, NULL, NULL)) != NULL)
        printf("%s\r\n", err);
        return -1;

    return 0;
}



