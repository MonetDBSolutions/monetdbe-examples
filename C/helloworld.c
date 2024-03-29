/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * (c) 2020- MonetDB Solutions B.V.
 *
 * This trivial program can be used to check if all the basic ingredients
 * for using MonetDBe has been available and accessible.
 *
 * For an explanation of the command arguments see ...
 */

#include "monetdbe.h"
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}

int
main(void)
{
	monetdbe_database mdbe = NULL;

	// second argument is a string for the db directory or NULL for in-memory mode
	if (monetdbe_open(&mdbe, "dbx", NULL))
		error("Failed to open database")

	printf("hello world, we have a lift off\n MonetDBe has been started\n");

	if (monetdbe_close(mdbe))
		error("Failed to close database")

	printf("hello world, we savely returned\n");
	return 0;
}
