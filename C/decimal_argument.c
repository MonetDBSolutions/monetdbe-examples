/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define expected_error(msg) {fprintf(stderr, "Failure: %s\n", msg); return 0;}
#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(int argc, char** argv)
{
    (void) argc;
    char* err = NULL;
    monetdbe_database mdbe = NULL;	

    if (monetdbe_open(&mdbe, NULL, NULL))
        expected_error("Failed to open database")
    if ((err = monetdbe_query(mdbe, "DROP TABLE IF EXISTS test; ", NULL, NULL)) != NULL)
        error(err)
    if ((err = monetdbe_query(mdbe, "CREATE TABLE test (x DECIMAL(15,5)); ", NULL, NULL)) != NULL)
        error(err)
    if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (10000.55555); ", NULL, NULL)) != NULL)
        error(err)
    if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (20000.55555); ", NULL, NULL)) != NULL)
        error(err)
    monetdbe_statement *stmt = NULL;
    monetdbe_result* result = NULL;
    if ((err = monetdbe_prepare(mdbe, "SELECT x FROM test WHERE x > ?; ", &stmt, NULL)) != NULL)
        error(err)
    long s = 1000055555;

    if ((err = monetdbe_bind(stmt, &s, 0)) != NULL)
        error(err)
    if ((err = monetdbe_execute(stmt, &result, NULL)) != NULL)
        error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
    for (int64_t r = 0; r < result->nrows; r++) {
        for (size_t c = 0; c < result->ncols; c++) {
            monetdbe_column* rcol;
            if ((err = monetdbe_result_fetch(result, &rcol, c)) != NULL)
                error(err)
            monetdbe_column_int64_t * col = (monetdbe_column_int64_t *) rcol;
            if (col->data[r] == col->null_value) {
                printf("NULL");
            } else {
                if (col->scale)
                    printf("%" PRId64 ".%03" PRId64, col->data[r]/(int64_t)col->scale, col->data[r]%(int64_t)col->scale);
                else
                    printf("%" PRId64, col->data[r]);
            }
        }
        printf("\n");
    }

    if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
        error(err)
    if (monetdbe_close(mdbe))
        error("Failed to close database")
    return 0;
}
