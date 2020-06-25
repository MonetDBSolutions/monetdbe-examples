#include "monetdbe.h"

// sf=0 will load only schema
char* dbgen(double sf, monetdbe_database mdbe, char* schema);

//! Gets the specified TPC-H Query number as a string
char* get_query(int query);

//char* get_answer(double sf, int query);
