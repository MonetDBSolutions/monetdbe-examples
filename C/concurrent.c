#include <monetdbe.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h> 
#include <pthread.h> 

void* write_tables(void* args) {
    (void) args;
    monetdbe_cnt rows_affected = 0;
    monetdbe_database db2 = NULL;

    if (monetdbe_open(&db2, NULL /* inmemory database */, NULL /* no options */)) {
        fprintf(stderr, "Failed to open database\n");
        return NULL;
    }

    if (monetdbe_query(db2, "INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);", NULL, &rows_affected) != NULL) {
        fprintf(stderr, "Failed to query database\n");
    }

    (void)monetdbe_close(db2);
    return NULL;
}

void* read_tables(void* args) {
    (void) args;
    monetdbe_database db3 = NULL;
    monetdbe_result* result;

    if (monetdbe_open(&db3, NULL /* inmemory database */, NULL /* no options */)) {
        fprintf(stderr, "Failed to open database\n");
        return NULL;
    }

    while (true) {
        if (monetdbe_query(db3, "SELECT * FROM integers", &result, NULL) != NULL) {
            fprintf(stderr, "Failed to query database\n");
            return NULL;
        }

        if (result->nrows == 0) {
            printf("The table is still empty.\n");
            monetdbe_cleanup_result(db3, result);
        }
        else
            break;
    }

    printf("The table finally has content:\n");
    monetdbe_column *cols[2];
    // print the names of the result
    for (size_t i = 0; i < result->ncols; i++) {
            monetdbe_result_fetch(result, &cols[i], i);
            printf("%s ", cols[i]->name);
    }

    printf("\n");
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

    monetdbe_cleanup_result(db3, result);
    (void)monetdbe_close(db3);
    return NULL;
}

int main(int argc, char **argv) 
{
    monetdbe_database db1 = NULL;

    if (monetdbe_open(&db1, NULL /* inmemory database */, NULL /* no options */)) {
        fprintf(stderr, "Failed to open database\n");
        return -1;
    }

    if (monetdbe_query(db1, "CREATE TABLE integers(i INTEGER, j INTEGER);", NULL, NULL) != NULL) {
        fprintf(stderr, "Failed to query database\n");
        return -1;
    }

    pthread_t tid1;
    pthread_t tid2;

    pthread_create(&tid1, NULL, write_tables, NULL);
    pthread_create(&tid2, NULL, read_tables, NULL);

    pthread_join(tid1, NULL);
    pthread_join(tid2, NULL);

    return monetdbe_close(db1);
}

