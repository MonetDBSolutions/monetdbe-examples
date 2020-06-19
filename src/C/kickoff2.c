/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * (c) 2020- MonetDB Solutions B.V.
 * This version of kickoff has been written to exercise the more enhanced result structure
 * references as a monetdbe_table structure
 * typedef struct {
   monetdb_cnt nrows;
   int ncols;
  int affectedrows;
  void *header[];  // the result set header
  void *data[];
} monetdb_table;
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void)
{
	char* err = NULL;
	monetdbe_database  db = NULL;		// a reference
	monetdbe_table result = NULL;		// a reference

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(NULL))
		error("Can not open database"))

	// ignore errors
	monetdbe_query(db, "CREATE TABLE test (x integer, y string)", NULL);
	monetdbe_query(db, "INSERT INTO test VALUES (42, 'Hello'), (NULL, 'World')", NULL);
	monetdbe_query(db, "SELECT x, y FROM test; ", &result);

	if( result->error)
		error(result->error);

	fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	for (int64_t r = 0; r < result->nrows; r++) {
		for (size_t c = 0; c < result->ncols; c++) {
			if ( monetdbe_isnull(result,c,r)) {
				printf("NULL");
				continue;
			}
			switch (result->header[c]->type) {
			case monetdbe_int32_t:
					printf("%d", result->data[c][r]);
					break;
			case monetdbe_str:
					printf("%s", (char*) result->data[c][r]);
					break;
			default: 
				printf("UNKNOWN");
			}

			if (c + 1 < result->ncols) {
				printf(", ");
			}
		}
		printf("\n");
	}

	if ((err = monetdbe_cleanup_result(result)) != NULL)
		error(err)
	if (monetdbe_close(db)) 
		error("Could not close database")
}
