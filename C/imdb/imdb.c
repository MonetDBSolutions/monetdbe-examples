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
#include <time.h>
#include "imdb.h"

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}
#define date_eq(d1, d2) (d1.year == d2.year && d1.month == d2.month && d1.day == d2.day)
#define time_eq(t1, t2) (t1.hours == t2.hours && t1.minutes == t2.minutes && t1.seconds == t2.seconds && t1.ms == t2.ms)

#define PRINT_COUNT(tbl) \
    sprintf(q, "select count(*) from sys.%s;", #tbl);\
    if ((err = monetdbe_query(mdbe, q, &result, NULL)) != NULL)\
    error(err);\
    if ((err=monetdbe_result_fetch(result, rcols, 0)) != NULL)\
    error(err);\
    col=(monetdbe_column_int64_t*) rcols[0];\
    r = (int64_t*) col->data;\
    fprintf(stdout, "select count(*) from %s -- [%d]\n", #tbl, *r);\
    if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)\
    error(err);

#define CLOCK_QUERY(qnum, qtext) \
    start = clock();\
    sprintf(q, "%s", qtext);\
    if ((err = monetdbe_query(mdbe, q, &result, NULL)) != NULL)\
        fprintf(stderr, "Failure: Qry[%s] -- %s\n\n", #qnum, err);\
    else{\
        end = clock();\
        printf("Q[%s] -- ", #qnum);\
        printf("took %f sec\n\n", (double)(end - start)/CLOCKS_PER_SEC);\
        if ((err = monetdbe_cleanup_result(mdbe, result)) != NULL)\
        error(err);\
    }

int main(int argc, char **argv)
{
    char* err = NULL;
    monetdbe_database mdbe = NULL;
    monetdbe_result* result = NULL;
    monetdbe_column* rcols[1];
    monetdbe_column_int64_t* col;
    int64_t* r;
    char* q;
    clock_t start, end;

    // second argument is a string for the db directory or NULL for in-memory mode
    if (monetdbe_open(&mdbe, NULL, NULL))
        error("Failed to open database");

    // try load schema
    printf("running imdb ...\n");
    //err = imdbgen(mdbe, "sys");
    //if (err)
    //    error(err);

    
   // const char* q1 = get_query(1);
   // CLOCK_QUERY(1, q1);

   // const char* q2 = get_query(2);
   // CLOCK_QUERY(2, q2);

   // const char* q3 = get_query(3);
   // CLOCK_QUERY(3, q3);

   // const char* q4 = get_query(4);
   // CLOCK_QUERY(4, q4);

   // const char* q5 = get_query(5);
   // CLOCK_QUERY(5, q5);

   // const char* q6 = get_query(6);
   // CLOCK_QUERY(6, q6);

   // const char* q7 = get_query(7);
   // CLOCK_QUERY(7, q7);
   // 
   // const char* q8 = get_query(8);
   // CLOCK_QUERY(8, q8);
   // 
   // const char* q9 = get_query(9);
   // CLOCK_QUERY(9, q9);
   // 
   // const char* q10 = get_query(10);
   // CLOCK_QUERY(10, q10);
   // 
   // const char* q11 = get_query(11);
   // CLOCK_QUERY(11, q11);

   // const char* q12 = get_query(12);
   // CLOCK_QUERY(12, q12);

   // const char* q13 = get_query(13);
   // CLOCK_QUERY(13, q13);

   // const char* q14 = get_query(14);
   // CLOCK_QUERY(14, q14);

   // const char* q15 = get_query(15);
   // CLOCK_QUERY(15, q15);

   // const char* q16 = get_query(16);
   // CLOCK_QUERY(16, q16);

   // const char* q17 = get_query(17);
   // CLOCK_QUERY(17, q17);

   // const char* q18 = get_query(18);
   // CLOCK_QUERY(18, q18);

   // const char* q19 = get_query(19);
   // CLOCK_QUERY(19, q19);

   // const char* q20 = get_query(20);
   // CLOCK_QUERY(20, q20);

   // const char* q21 = get_query(21);
   // CLOCK_QUERY(21, q21);

   // const char* q22 = get_query(22);
   // CLOCK_QUERY(22, q22);
    
    if (monetdbe_close(mdbe))
        error("Failed to close database");

    fprintf(stdout, "Done\n");
    return 0;
}
