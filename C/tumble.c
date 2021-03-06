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

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) { fprintf(stderr, "Failure: %s\n", msg); exit(-1); }
monetdbe_database mdbe = NULL;

void startup(char *db)
{
	if (monetdbe_open(&mdbe, NULL, NULL))
		error("Failed to open database")
}

void _shutdown()
{
	if (monetdbe_close(mdbe))
		error("Failed to close database")
}

int
main(void)
{
	printf("hello all, startup your first database\n");
	startup(NULL);
	_shutdown();

	printf("closed it\n, try a persistent one");
	startup("./_local_");
	_shutdown();

	printf("that was all for today\n");
	return 0;
}
