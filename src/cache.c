/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * (c) 2020- MonetDB Solutions B.V.
 *
 * An embedded application can act a cache with a remote server.
 * For this we need two database objects, one :inmemory: and a link to the remote.
 *
 * For an explanation of the command arguments see ...
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); exit -1;}


int
main(void)
{
    monetdbe_database local = NULL, remote = NULL;
    monetdbe_result *data, *schema, *result;

    monetdbe_open_remote('sf1', 'monetdb', 'monetdb', 'localhost',50000, &remote);  
    monetdbe_query(remote, "call sys.describe(\"sys\",\"lineitem\"", &schema); 
    monetdbe_query(remote, "select line_no from sf1 where line_no < 10", &data);
    
    if( remote->error || schema->error || data->error)
        error("Could not access the remote database");
	// query returns a single string
	schemadef = (char*) schema->data[0][0];

    // store the result in an :inmemory: structure
    monetdbe_open(NULL,0, &local);  
    monetdbe_query(local, schemadef, &schema);
    monetdbe_append(local, "sys","lineitem", data, &result);

    if( local->error || schema->error || result->error )
        error("Construction of the local cache failed\n");

	printf("Obtained %d tuples from the remote", result->affectedrows);
}
