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

/* 
 * This program assumes a running mserver5 instance listening on localhost interface.
 * Signature:
 * 				proxy <database> <port>.
 */
int
main(int argc, char** argv)
{
	(void) argc;
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	assert(argc==3);
	const int port = strtol(argv[1], NULL, 10);
	const char* database = argv[2];
	/* Because of the start up order, this test (which connects too itself, doesn't run. */
	monetdbe_remote remote = {.host="localhost", .port=port, .database=database, .username="monetdb", .password="monetdb"};
	monetdbe_options opts = {.remote = &remote};

	if (monetdbe_open(&mdbe, NULL, &opts))
		expected_error("Failed to open database")
	if ((err = monetdbe_query(mdbe, "DROP TABLE IF EXISTS test; ", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "CREATE TABLE test (x INT, y STRING); ", NULL, NULL)) != NULL)
		error(err)
	if ((err = monetdbe_query(mdbe, "INSERT INTO test VALUES (10, 'foo'), (20, 'bar'); ", NULL, NULL)) != NULL)
		error(err)

	monetdbe_result* result = NULL;

	monetdbe_statement *stmt = NULL;
	if ((err = monetdbe_prepare(mdbe, "SELECT x, y FROM test WHERE x > ?; ", &stmt, &result)) != NULL)
		error(err)
	monetdbe_column* rcol[result->ncols];
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			if ((err = monetdbe_result_fetch(result, rcol+c, c)) != NULL)
				error(err)
			switch (rcol[c]->type) {
				case monetdbe_int32_t: {
					monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol[c];
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_str: {
					monetdbe_column_str * col = (monetdbe_column_str *) rcol[c];
					if (col->is_null(col->data+r)) {
						printf("NULL");
					} else {
						printf("%s", (char*) col->data[r]);
					}
					break;
				}
				default: {
					printf("UNKNOWN");
				}
			}

			if (c + 1 < result->ncols) {
				printf(", ");
			}
		}
		printf("\n");
	}
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)

	unsigned s = 10;
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
			switch (rcol->type) {
				case monetdbe_int32_t: {
					monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol;
					if (col->data[r] == col->null_value) {
						printf("NULL");
					} else {
						printf("%d", col->data[r]);
					}
					break;
				}
				case monetdbe_str: {
					monetdbe_column_str * col = (monetdbe_column_str *) rcol;
					if (col->is_null(col->data+r)) {
						printf("NULL");
					} else {
						printf("%s", (char*) col->data[r]);
					}
					break;
				}
				default: {
					printf("UNKNOWN");
				}
			}

			if (c + 1 < result->ncols) {
				printf(", ");
			}
		}
		printf("\n");
	}
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)

	return 0;
}
