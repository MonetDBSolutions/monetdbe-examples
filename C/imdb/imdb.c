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
    
    if (argc < 2) {
	    fprintf(stderr, "location of the imdb data files is missing\n");
	    return -1;
    }
    char* csv_path = argv[1];

    // second argument is a string for the db directory or NULL for in-memory mode
    if (monetdbe_open(&mdbe, NULL, NULL))
        error("Failed to open database");
    
    start = clock();
    printf("loading data ...\n");
    // try load schema
    err = imdbgen(mdbe, csv_path);
    if (err)
        error(err);
    end = clock();
    printf("took %f sec\n\n", (double)(end - start)/CLOCKS_PER_SEC);\
    
   // run 114 queries

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
   CLOCK_QUERY(23, (char*) get_query(23));
   CLOCK_QUERY(24, (char*) get_query(24));
   CLOCK_QUERY(25, (char*) get_query(25));
   CLOCK_QUERY(26, (char*) get_query(26));
   CLOCK_QUERY(27, (char*) get_query(27));
   CLOCK_QUERY(28, (char*) get_query(28));
   CLOCK_QUERY(29, (char*) get_query(29));
   CLOCK_QUERY(30, (char*) get_query(30));
   CLOCK_QUERY(31, (char*) get_query(31));
   CLOCK_QUERY(32, (char*) get_query(32));
   CLOCK_QUERY(33, (char*) get_query(33));
   CLOCK_QUERY(34, (char*) get_query(34));
   CLOCK_QUERY(35, (char*) get_query(35));
   CLOCK_QUERY(36, (char*) get_query(36));
   CLOCK_QUERY(37, (char*) get_query(37));
   CLOCK_QUERY(38, (char*) get_query(38));
   CLOCK_QUERY(39, (char*) get_query(39));
   CLOCK_QUERY(40, (char*) get_query(40));
   CLOCK_QUERY(41, (char*) get_query(41));
   CLOCK_QUERY(42, (char*) get_query(42));
   CLOCK_QUERY(43, (char*) get_query(43));
   CLOCK_QUERY(44, (char*) get_query(44));
   CLOCK_QUERY(45, (char*) get_query(45));
   CLOCK_QUERY(46, (char*) get_query(46));
   CLOCK_QUERY(47, (char*) get_query(47));
   CLOCK_QUERY(48, (char*) get_query(48));
   CLOCK_QUERY(49, (char*) get_query(49));
   CLOCK_QUERY(50, (char*) get_query(50));
   CLOCK_QUERY(51, (char*) get_query(51));
   CLOCK_QUERY(52, (char*) get_query(52));
   CLOCK_QUERY(53, (char*) get_query(53));
   CLOCK_QUERY(54, (char*) get_query(54));
   CLOCK_QUERY(55, (char*) get_query(55));
   CLOCK_QUERY(56, (char*) get_query(56));
   CLOCK_QUERY(57, (char*) get_query(57));
   CLOCK_QUERY(58, (char*) get_query(58));
   CLOCK_QUERY(59, (char*) get_query(59));
   CLOCK_QUERY(60, (char*) get_query(60));
   CLOCK_QUERY(61, (char*) get_query(61));
   CLOCK_QUERY(62, (char*) get_query(62));
   CLOCK_QUERY(63, (char*) get_query(63));
   CLOCK_QUERY(64, (char*) get_query(64));
   CLOCK_QUERY(65, (char*) get_query(65));
   CLOCK_QUERY(66, (char*) get_query(66));
   CLOCK_QUERY(67, (char*) get_query(67));
   CLOCK_QUERY(68, (char*) get_query(68));
   CLOCK_QUERY(69, (char*) get_query(69));
   CLOCK_QUERY(70, (char*) get_query(70));
   CLOCK_QUERY(71, (char*) get_query(71));
   CLOCK_QUERY(72, (char*) get_query(72));
   CLOCK_QUERY(73, (char*) get_query(73));
   CLOCK_QUERY(74, (char*) get_query(74));
   CLOCK_QUERY(75, (char*) get_query(75));
   CLOCK_QUERY(76, (char*) get_query(76));
   CLOCK_QUERY(77, (char*) get_query(77));
   CLOCK_QUERY(78, (char*) get_query(78));
   CLOCK_QUERY(79, (char*) get_query(79));
   CLOCK_QUERY(80, (char*) get_query(80));
   CLOCK_QUERY(81, (char*) get_query(81));
   CLOCK_QUERY(82, (char*) get_query(82));
   CLOCK_QUERY(83, (char*) get_query(83));
   CLOCK_QUERY(84, (char*) get_query(84));
   CLOCK_QUERY(85, (char*) get_query(85));
   CLOCK_QUERY(86, (char*) get_query(86));
   CLOCK_QUERY(87, (char*) get_query(87));
   CLOCK_QUERY(88, (char*) get_query(88));
   CLOCK_QUERY(89, (char*) get_query(89));
   CLOCK_QUERY(90, (char*) get_query(90));
   CLOCK_QUERY(91, (char*) get_query(91));
   CLOCK_QUERY(92, (char*) get_query(92));
   CLOCK_QUERY(93, (char*) get_query(93));
   CLOCK_QUERY(94, (char*) get_query(94));
   CLOCK_QUERY(95, (char*) get_query(95));
   CLOCK_QUERY(96, (char*) get_query(96));
   CLOCK_QUERY(97, (char*) get_query(97));
   CLOCK_QUERY(98, (char*) get_query(98));
   CLOCK_QUERY(99, (char*) get_query(99));
   CLOCK_QUERY(100, (char*) get_query(100));
   CLOCK_QUERY(101, (char*) get_query(101));
   CLOCK_QUERY(102, (char*) get_query(102));
   CLOCK_QUERY(103, (char*) get_query(103));
   CLOCK_QUERY(104, (char*) get_query(104));
   CLOCK_QUERY(105, (char*) get_query(105));
   CLOCK_QUERY(106, (char*) get_query(106));
   CLOCK_QUERY(107, (char*) get_query(107));
   CLOCK_QUERY(108, (char*) get_query(108));
   CLOCK_QUERY(109, (char*) get_query(109));
   CLOCK_QUERY(110, (char*) get_query(110));
   CLOCK_QUERY(111, (char*) get_query(111));
   CLOCK_QUERY(112, (char*) get_query(112));
   CLOCK_QUERY(113, (char*) get_query(113));
   CLOCK_QUERY(114, (char*) get_query(114));
    
    if (monetdbe_close(mdbe))
        error("Failed to close database");

    fprintf(stdout, "Done\n");
    return 0;
   
}
