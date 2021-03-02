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
#ifndef WIN32
#include <unistd.h>
#endif
#include <limits.h>
#include <string.h>
#include <time.h>
#include "dbgen.h"

// #define DEBUG_DATA
#define sf 1

#define error(msg) {fprintf(stderr, "Failure: %s\n", msg); return -1;}
#define date_eq(d1, d2) (d1.year == d2.year && d1.month == d2.month && d1.day == d2.day)
#define time_eq(t1, t2) (t1.hours == t2.hours && t1.minutes == t2.minutes && t1.seconds == t2.seconds && t1.ms == t2.ms)

static char hexit[] = "0123456789ABCDEF";
int debug_data(monetdbe_result*, char*);

#ifdef DEBUG_DATA
int 
debug_data(monetdbe_result* result, char* err) {
    monetdbe_column** rcol = (monetdbe_column**) malloc(sizeof(monetdbe_column*) * result->ncols);
    if(!rcol)
        error("debug_data malloc failed\n");

    for (int64_t r = 0; r < result->nrows; r++) {
        for (size_t c = 0; c < result->ncols; c++) {
            if ((err = monetdbe_result_fetch(result, rcol+c, c)) != NULL)
                error(err)
                    switch (rcol[c]->type) {
                        case monetdbe_int32_t: {
                                                   monetdbe_column_int32_t * col = (monetdbe_column_int32_t *) rcol[c];
                                                   if (col->data[r] == col->null_value) {
                                                       printf("NULL");
                                                   } else {
                                                       printf("%d", col->data[r]);
                                                   }
                                                   break;
                                               }
                        case monetdbe_str: {
                                               monetdbe_column_str * col = (monetdbe_column_str *) rcol[c];
                                               if (col->is_null(col->data+r)) {
                                                   printf("NULL");
                                               } else {
                                                   printf("%s", (char*) col->data[r]);
                                               }
                                               break;
                                           }
                        case monetdbe_date: {
                                                monetdbe_column_date * col = (monetdbe_column_date *) rcol[c];
                                                if (date_eq(col->data[r], col->null_value)) {
                                                    printf("NULL");
                                                } else {
                                                    printf("%d-%d-%d", col->data[r].year, col->data[r].month, col->data[r].day);
                                                }
                                                break;
                                            }
                        case monetdbe_time: {
                                                monetdbe_column_time * col = (monetdbe_column_time *) rcol[c];
                                                if (time_eq(col->data[r], col->null_value)) {
                                                    printf("NULL");
                                                } else {
                                                    printf("%d:%d:%d.%d", col->data[r].hours, col->data[r].minutes, col->data[r].seconds, col->data[r].ms);
                                                }
                                                break;
                                            }
                        case monetdbe_timestamp: {
                                                     monetdbe_column_timestamp * col = (monetdbe_column_timestamp *) rcol[c];
                                                     if (date_eq(col->data[r].date, col->null_value.date) && time_eq(col->data[r].time, col->null_value.time)) {
                                                         printf("NULL");
                                                     } else {
                                                         printf("%d-%d-%d ", col->data[r].date.year, col->data[r].date.month, col->data[r].date.day);
                                                         printf("%d:%d:%d.%d", col->data[r].time.hours, col->data[r].time.minutes, col->data[r].time.seconds, col->data[r].time.ms);
                                                     }
                                                     break;
                                                 }
                        case monetdbe_blob: {
                                                monetdbe_column_blob * col = (monetdbe_column_blob *) rcol[c];
                                                if (!col->data[r].data) {
                                                    printf("NULL");
                                                } else {
                                                    for (size_t i = 0; i < col->data[r].size; i++) {
                                                        int hval = (col->data[r].data[i] >> 4) & 15;
                                                        int lval = col->data[r].data[i] & 15;

                                                        printf("%c%c", hexit[hval], hexit[lval]);
                                                    }
                                                }
                                                break;
                                            }
                        default: {
                                     printf("UNKNOWN");
                                 }
                    }

            if (c + 1 < result->ncols) {
                printf(", ");
            }
        }
        printf("\n");
    }

    free(rcol);

    return 0;
}
#endif

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
    if ((err = monetdbe_query(mdbe, qtext, &result, NULL)) != NULL)\
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
    err = dbgen(sf, mdbe, "sys");
    if (err)
        error(err);

    PRINT_COUNT(region);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(nation);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(customer); 
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(supplier);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(part);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(partsupp);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif
    PRINT_COUNT(orders);
#ifdef DEBUG_DATA
    debug_data(result, err);
#endif

    PRINT_COUNT(lineitem);
    
    CLOCK_QUERY(1, (char*) get_query(1));
    CLOCK_QUERY(2, (char*) get_query(2));
    CLOCK_QUERY(3, (char*) get_query(3));
    CLOCK_QUERY(4, (char*) get_query(4));
    CLOCK_QUERY(5, (char*) get_query(5));
    CLOCK_QUERY(6, (char*) get_query(6));
    CLOCK_QUERY(7, (char*) get_query(7));
    CLOCK_QUERY(8, (char*) get_query(8));
    CLOCK_QUERY(9, (char*) get_query(9));
    CLOCK_QUERY(10, (char*) get_query(10));
    CLOCK_QUERY(11, (char*) get_query(11));
    CLOCK_QUERY(12, (char*) get_query(12));
    CLOCK_QUERY(13, (char*) get_query(13));
    CLOCK_QUERY(14, (char*) get_query(14));
    CLOCK_QUERY(15, (char*) get_query(15));
    CLOCK_QUERY(16, (char*) get_query(16));
    CLOCK_QUERY(17, (char*) get_query(17));
    CLOCK_QUERY(18, (char*) get_query(18));
    CLOCK_QUERY(19, (char*) get_query(19));
    CLOCK_QUERY(20, (char*) get_query(20));
    CLOCK_QUERY(21, (char*) get_query(21));
    CLOCK_QUERY(22, (char*) get_query(22));
    
    if (monetdbe_close(mdbe))
        error("Failed to close database");

    fprintf(stdout, "Done\n");
    return 0;
}
