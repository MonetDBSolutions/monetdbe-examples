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

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;

	// second argument is a string for the db directory or NULL for in-memory mode
	if ((err = monetdbe_open(&mdbe, NULL, NULL)) != NULL)
		error(err)

    char* ddl ="CREATE TABLE logs (log_time TIMESTAMP DEFAULT NOW, cpu_user FLOAT);";
    char* sql = "insert into logs (cpu_user) values (0.2), (0.3), (0.5);";
    
    if ((err = monetdbe_query(mdbe, (char*)ddl, NULL, NULL)) != NULL)
		error(err)
    if ((err = monetdbe_query(mdbe, (char*)sql, NULL, NULL)) != NULL)
		error(err)

	if ((err = monetdbe_query(mdbe, "select count(*) from logs;", &result, NULL)) != NULL)
		error(err)
    
    fprintf(stdout, "Query result with %zu cols and %"PRId64" rows\n", result->ncols, result->nrows);
	if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)
		error(err)
    int autocommit;
    int* ac = &autocommit; 
	if ((err = monetdbe_get_autocommit(mdbe, (int*)ac)) != NULL)
		error(err)
    fprintf(stdout, "autocommit is %d!\n", autocommit);
    
    // testing dump
	if ((err = monetdbe_dump_database(mdbe, "/tmp/logs_dump")) != NULL)
		error(err)
	
    if ((err = monetdbe_close(mdbe)) != NULL)
		error(err)
	return 0;
}
