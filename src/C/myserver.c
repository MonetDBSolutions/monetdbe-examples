/*
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 Create your own database server, initialized with some data
 After it has started you can connect to it using JDBC/ODCB, MAPI, or simply mclient
*/

#include "monetdbe.h"
#include <stdio.h>

int main() {
	monetdbe_database db = NULL;

	/* connect to a :memory: database */
	if (monetdbe_open(&db, NULL /* inmemory database */, NULL /* no options */)) {
		fprintf(stderr, "Failed to open database\n");
		return -1;
	}

	
	/* open up as a database server by listening to TCP/IP connections */

	if( monetdbe_listen(&db, "localhost", 50033))
		fprintf(stderr, "ERROR %s", monetdb_error(db));

	/* continuously accept connections */
	for(;;){
		sleep(20);
		fprintf(stderr, "%s still running\n", argv[0]);
	}
}
