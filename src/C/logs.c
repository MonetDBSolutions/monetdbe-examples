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


#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

char *
load(monetdbe_database mdbe, char* csv_path) {
	char* err = NULL;
    char* ddl = "\
        CREATE TABLE logs (\
            log_time      TIMESTAMP NOT NULL,\
            machine_name  VARCHAR(25) NOT NULL,\
            machine_group VARCHAR(15) NOT NULL,\
            cpu_idle      FLOAT,\
            cpu_nice      FLOAT,\
            cpu_system    FLOAT,\
            cpu_user      FLOAT,\
            cpu_wio       FLOAT,\
            disk_free     FLOAT,\
            disk_total    FLOAT,\
            part_max_used FLOAT,\
            load_fifteen  FLOAT,\
            load_five     FLOAT,\
            load_one      FLOAT,\
            mem_buffers   FLOAT,\
            mem_cached    FLOAT,\
            mem_free      FLOAT,\
            mem_shared    FLOAT,\
            swap_free     FLOAT,\
            bytes_in      FLOAT,\
            bytes_out     FLOAT\
            );\
        ";

    if((err=monetdbe_query(mdbe, (char*)ddl, NULL, NULL)) != NULL)
        return err;
    char* head="COPY 1000000 OFFSET 2 RECORDS INTO logs FROM '";
    char* tail = "' USING DELIMITERS ',' NULL AS ''";
    char sql[1000];
    strcpy(sql, head);
    strcat(sql, csv_path);
    strcat(sql, tail);
    printf("%s\n", sql);
    if((err=monetdbe_query(mdbe, sql, NULL, NULL)) != NULL)
        return err;
    return NULL;
}

int
main(int argc, char **argv)
{
	char* err = NULL;
	monetdbe_database mdbe = NULL;
	monetdbe_result* result = NULL;
    
    if (argc < 2) {
	    fprintf(stderr, "location of the benchmark file (bench1.csv) missing\n");
	    return -1;
    }
    char* csv_path = argv[1];

    // second argument is a string for the db directory or NULL for in-memory mode
    if (monetdbe_open(&mdbe, NULL, NULL))
	error("Failed to open database")
    
    if ((err=load(mdbe, csv_path)) != NULL)
        error(err)

    char* sql="\
        SELECT machine_name,\
        MIN(cpu) AS cpu_min,\
        MAX(cpu) AS cpu_max,\
        AVG(cpu) AS cpu_avg,\
        MIN(net_in) AS net_in_min,\
        MAX(net_in) AS net_in_max,\
        AVG(net_in) AS net_in_avg,\
        MIN(net_out) AS net_out_min,\
        MAX(net_out) AS net_out_max,\
        AVG(net_out) AS net_out_avg\
        FROM (\
        SELECT machine_name,\
                COALESCE(cpu_user, 0.0) AS cpu,\
                COALESCE(bytes_in, 0.0) AS net_in,\
                COALESCE(bytes_out, 0.0) AS net_out\
        FROM logs\
        WHERE machine_name IN ('anansi','aragog','urd')\
            AND log_time >= TIMESTAMP '2017-01-11 00:00:00'\
        ) AS r\
        GROUP BY machine_name;\
    ";
	if ((err = monetdbe_query(mdbe, sql, &result, NULL)) != NULL)
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
	
    if (monetdbe_close(mdbe))
		error("Failed to close database")
	return 0;
}
