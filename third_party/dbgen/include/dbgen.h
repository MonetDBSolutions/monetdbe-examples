#include "monetdbe.h"

// flt_scale=0 will load only schema
char* dbgen(double flt_scale, monetdbe_database mdbe, char* schema);

//! Gets the specified TPC-H Query number as a string
const char* get_query(int query);
const char* get_answer(int query);
