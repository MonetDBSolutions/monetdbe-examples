/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * (c) 2020- MonetDB Solutions B.V.
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <unistd.h>
#include <limits.h>
#include <string.h>
#include "dbgen.h"

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

#define sf 1

int
main(int argc, char **argv)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;
    
    // second argument is a string for the db directory or NULL for in-memory mode
    if (monetdbe_open(&mdbe, NULL, NULL))
	    error("Failed to open database")

    // try load schema
    dbgen(sf, mdbe, "sys");
    
	if ((err = monetdbe_query(mdbe, "select * from sys.region;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);
	
    if ((err = monetdbe_query(mdbe, "select * from sys.nation;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);
	
    if ((err = monetdbe_query(mdbe, "select * from sys.supplier;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);

    if ((err = monetdbe_query(mdbe, "select * from sys.customer;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);

    if ((err = monetdbe_query(mdbe, "select * from sys.part;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);

    if ((err = monetdbe_query(mdbe, "select * from sys.partsupp;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);
    
    if ((err = monetdbe_query(mdbe, "select * from sys.orders;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);

    if ((err = monetdbe_query(mdbe, "select * from sys.lineitem;", &result, NULL)) != NULL)
		error(err)
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows and name %s\n", result->ncols, result->nrows, result->name);

    if (monetdbe_close(mdbe))
		error("Failed to close database")

    printf("Done\n");
	return 0;
}
