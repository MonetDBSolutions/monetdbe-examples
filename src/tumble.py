/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * (c) 2020- MonetDB Solutions B.V.
 *
 * Within an embedded application should be able to switch
 * between database at will. Ofcourse, simple switching with
 * another in-memory database will loose the content of the first call.
 * If you want to retain the information, just use a local database.
 *
 * For an explanation of the command arguments see ...
 */

#include "monetdb_embedded.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); exit -1;}
monetdb_connection conn = NULL;

void startup(char *db)
{
	char* err = NULL;
	if ((err = monetdb_startup(db, 0)) != NULL)
		error(err)
	if ((err = monetdb_connect(&conn)) != NULL)
		error(err)
}

void shutdown(char *db)
{
	char* err = NULL;
	if ((err = monetdb_disconnect(conn)) != NULL)
		error(err)
	if ((err = monetdb_shutdown()) != NULL)
		error(err)
}

int
main(void)
{
	printf("hello all, startup your first database\n");
	startup(NULL);
	shutdown(NULL);
	printf("closed it\n, try a persistent one");
	startup("./_monetdbe_db_");
	shutdown();
	printf("that was all for today\n");
	return 0;
}
