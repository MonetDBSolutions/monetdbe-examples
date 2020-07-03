/* main driver for dss banchmark */

#define DECLARER				/* EXTERN references get defined here */
#define NO_FUNC (int (*) ()) NULL	/* to clean up tdefs */
#define NO_LFUNC (long (*) ()) NULL		/* to clean up tdefs */

#include "config.h"
#include "release.h"
#include <stdlib.h>
#if (defined(_POSIX_)||!defined(WIN32))		/* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif /* WIN32 */
#include <stdio.h>				/* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32)&&!defined(_POSIX_))
#include <process.h>
#pragma warning(disable:4201)
#pragma warning(disable:4214)
#pragma warning(disable:4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default:4201)
#pragma warning(default:4214)
#endif

#include "dss.h"
#include "dsstypes.h"
#include "dbgen.h"
#define debug(fmt, ...) printf("%s:%d: " fmt, __FILE__, __LINE__, __VA_ARGS__);

extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif
static int bTableSet = 0;

extern seed_t Seed[];
seed_t seed_backup[MAX_STREAM + 1];
static bool first_invocation = true;

/*
* general table descriptions. See dss.h for details on structure
* NOTE: tables with no scaling info are scaled according to
* another table
*
*
* the following is based on the tdef structure defined in dss.h as:
* typedef struct
* {
* char     *name;            -- name of the table; 
*                               flat file output in <name>.tbl
* long      base;            -- base scale rowcount of table; 
*                               0 if derived
* int       (*loader) ();    -- function to present output
* long      (*gen_seed) ();  -- functions to seed the RNG
* int       child;           -- non-zero if there is an associated detail table
* unsigned long vtotal;      -- "checksum" total 
* }         tdef;
*
*/

tdef tdefs[] = {
    {"part.tbl", "part table", 200000, NULL, NULL, PSUPP, 0},
    {"partsupp.tbl", "partsupplier table", 200000, NULL, NULL, NONE, 0},
    {"supplier.tbl", "suppliers table", 10000, NULL, NULL, NONE, 0},
    {"customer.tbl", "customers table", 150000, NULL, NULL, NONE, 0},
    {"orders.tbl", "order table", 150000, NULL, NULL, LINE, 0},
    {"lineitem.tbl", "lineitem table", 150000, NULL, NULL, NONE, 0},
    {"orders.tbl", "orders/lineitem tables", 150000, NULL, NULL, LINE, 0},
    {"part.tbl", "part/partsupplier tables", 200000, NULL, NULL, PSUPP, 0},
    {"nation.tbl", "nation table", NATIONS_MAX, NULL, NULL, NONE, 0},
    {"region.tbl", "region table", NATIONS_MAX, NULL, NULL, NONE, 0},
};


/*
* read the distributions needed in the benchamrk
*/
void
load_dists (void)
{
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_cntr", &p_cntr_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "colors", &colors);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "p_types", &p_types_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nations", &nations);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "regions", &regions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "o_oprio",
		&o_priority_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "instruct",
		&l_instruct_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "smode", &l_smode_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "category",
		&l_category_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "rflag", &l_rflag_set);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "msegmnt", &c_mseg_set);

	/* load the distributions that contain text generation */
	read_dist (env_config (DIST_TAG, DIST_DFLT), "nouns", &nouns);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "verbs", &verbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adjectives", &adjectives);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "adverbs", &adverbs);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "auxillaries", &auxillaries);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "terminators", &terminators);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "articles", &articles);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "prepositions", &prepositions);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "grammar", &grammar);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "np", &np);
	read_dist (env_config (DIST_TAG, DIST_DFLT), "vp", &vp);
	
}

typedef struct append_info_t {
    size_t ncols;
    monetdbe_column** cols;
    size_t counter;
    bool init;
} append_info_t;

typedef struct tpch_info_t {
    append_info_t PART_INFO;
    append_info_t PSUPP_INFO;
    append_info_t SUPP_INFO;
    append_info_t CUST_INFO;
    append_info_t ORDER_INFO;
    append_info_t LINE_INFO;
    append_info_t NATION_INFO;
    append_info_t REGION_INFO;
} tpch_info_t;


/*
* Function prototypes
*/
static void	gen_tbl (int tnum,  DSS_HUGE count, tpch_info_t*);
static void	init_tbl (int tnum,  DSS_HUGE count, tpch_info_t*);

static void* Zalloc(monetdbe_types t, DSS_HUGE n) {
    switch(t) {
        case monetdbe_bool:
            return malloc(sizeof(bool)*n);
        case monetdbe_int8_t: 
            return malloc(sizeof(int8_t)*n);
        case monetdbe_int16_t: 
            return malloc(sizeof(int16_t)*n);
        case monetdbe_int32_t: 
            return malloc(sizeof(int32_t)*n);
        case monetdbe_int64_t: 
            return malloc(sizeof(int64_t)*n);
        case monetdbe_float: 
            return malloc(sizeof(float)*n);
        case monetdbe_double:
            return malloc(sizeof(double)*n);
        case monetdbe_str: 
            // TODO  FIX
            return malloc(sizeof(char**)*n);
        case monetdbe_blob: 
            return malloc(sizeof(monetdbe_data_blob)*n);
        case monetdbe_date: 
            return malloc(sizeof(monetdbe_data_date)*n);
        case monetdbe_time:
            return malloc(sizeof(monetdbe_data_time)*n);
        case monetdbe_timestamp:
            return malloc(sizeof(monetdbe_data_timestamp)*n);
        default:
            return NULL;
    }
}
static void init_info(append_info_t* t, DSS_HUGE count) {
    if (!(t->init)) {
        for(size_t i=0; i < (t->ncols); i++) {
            t->cols[i]->count = count;
            t->cols[i]->data = Zalloc(t->cols[i]->type, count);
            t->init=true;
        }
    }
}

static void append_str(void* buff, char* str) {
    size_t buff_length = (buff == NULL) ? 0 : sizeof(buff);
    size_t str_length = sizeof(str) + 1;
    size_t out_length = buff_length + str_length;
    char* out[out_length];
    memcpy(out, buff, buff_length);
    memcpy(out + buff_length, str, str_length + 1);
    buff = out;
}

static void append_region(code_t* c, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "r_regionkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = c->code; 
             continue;
         }
         if(strcmp(t->cols[i]->name, "r_name") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->text); 
             //append_str(t->cols[i]->data, c->text);
             continue;
         }
         if(strcmp(t->cols[i]->name, "r_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->comment); 
             // append_str(t->cols[i]->data, c->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_nation(code_t* c, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "n_nationkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = c->code; 
             continue;
         }
         if(strcmp(t->cols[i]->name, "n_name") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->text); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "n_regionkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = c->join; 
             continue;
         }
         if(strcmp(t->cols[i]->name, "n_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_supp(supplier_t* s, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "s_suppkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = s->suppkey;
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_name") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(s->name); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_address") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(s->address); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_nationkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = s->nation_code; 
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_phone") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(s->phone);
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_acctbal") == 0){
             ((double*)t->cols[i]->data)[k] = s->acctbal;
             continue;
         }
         if(strcmp(t->cols[i]->name, "s_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(s->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_cust(customer_t* c, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "c_custkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = c->custkey;
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_name") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->name); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_address") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->address); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_nationkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = c->nation_code; 
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_phone") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->phone);
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_acctbal") == 0){
             ((double*)t->cols[i]->data)[k] = c->acctbal;
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_mktsegment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->mktsegment);
             continue;
         }
         if(strcmp(t->cols[i]->name, "c_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(c->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_part(part_t* p, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "p_partkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = p->partkey;
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_name") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->name); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_mfgr") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->mfgr); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_brand") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->brand);
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_type") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->type);
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_size") == 0){
             ((int64_t*)t->cols[i]->data)[k] = p->size;
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_container") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->container);
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_retailprice") == 0){
             ((double*)t->cols[i]->data)[k] = p->retailprice;
             continue;
         }
         if(strcmp(t->cols[i]->name, "p_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(p->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_psupp(part_t* p, append_info_t* t) {
    size_t k = t->counter;
    
	for (size_t j = 0; j < SUPP_PER_PART; j++) {
        for (size_t i=0; i < (t->ncols); i++) {
             if(strcmp(t->cols[i]->name, "ps_partkey") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = p->s[j].partkey;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "ps_suppkey") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = p->s[j].suppkey;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "ps_availqty") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = p->s[j].qty;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "ps_supplycost") == 0){
                 ((double*)t->cols[i]->data)[k] = p->s[j].scost;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "ps_comment") == 0){
                 if (p->s[j].comment !=NULL)
                  ((char**)t->cols[i]->data)[k] = strdup(p->s[j].comment);
                 continue;
             }
             assert(false);
       }
        t->counter++;
   }
}

static monetdbe_data_date d_conv(char* date) {
    char* err=NULL;
    int year,month,day;
    sscanf(date, "%d-%d-%d", &year, &month, &day);
   // struct tm tm;
   // if((err=strptime(date, "%Y-%M-%d", &tm)) == NULL){
   //     fprintf(stderr, "strptime err with date %s", date);
   //     exit(1);
   // }
   // monetdbe_data_date d = {.day=tm.tm_mday, .month=tm.tm_mon, .year=tm.tm_year};
    monetdbe_data_date d = {.day=day, .month=month, .year=year};
    return d;
}

static void append_order(order_t* o, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
         if(strcmp(t->cols[i]->name, "o_orderkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = o->okey;
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_custkey") == 0){
             ((int64_t*)t->cols[i]->data)[k] = o->custkey;
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_orderstatus") == 0){
             ((char*)t->cols[i]->data)[k] = (o->orderstatus); 
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_totalprice") == 0){
             ((double*)t->cols[i]->data)[k] = o->totalprice;
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_orderdate") == 0){
             ((monetdbe_data_date*)t->cols[i]->data)[k] = d_conv(o->odate);
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_orderpriority") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(o->opriority);
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_clerk") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(o->clerk);
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_shippriority") == 0){
             ((int64_t*)t->cols[i]->data)[k] = o->spriority;
             continue;
         }
         if(strcmp(t->cols[i]->name, "o_comment") == 0){
             ((char**)t->cols[i]->data)[k] = strdup(o->comment);
             continue;
         }
         assert(false);
   }
    t->counter++;
}

static void append_line(order_t* o, append_info_t* t) {
    size_t k = t->counter;
	for (DSS_HUGE j = 0; j < o->lines; j++) {
        for (size_t i=0; i < (t->ncols); i++) {
             if(strcmp(t->cols[i]->name, "l_orderkey") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = o->l[j].okey;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_partkey") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = o->l[j].partkey;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_suppkey") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = o->l[j].suppkey;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_linenumber") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = o->l[j].lcnt;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_quantity") == 0){
                 ((int64_t*)t->cols[i]->data)[k] = o->l[j].quantity;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_extendedprice") == 0){
                 ((double*)t->cols[i]->data)[k] = o->l[j].eprice;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_discount") == 0){
                 ((double*)t->cols[i]->data)[k] = o->l[j].discount;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_tax") == 0){
                 ((double*)t->cols[i]->data)[k] = o->l[j].tax;
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_returnflag") == 0){
                 ((char*)t->cols[i]->data)[k] = ((o->l[j].rflag[0]));
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_linestatus") == 0){
                 ((char*)t->cols[i]->data)[k] = (o->l[j].lstatus[0]);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_shipdate") == 0){
                 ((monetdbe_data_date*)t->cols[i]->data)[k] = d_conv(o->l[j].sdate);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_commitdate") == 0){
                 ((monetdbe_data_date*)t->cols[i]->data)[k] = d_conv(o->l[j].cdate);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_receiptdate") == 0){
                 ((monetdbe_data_date*)t->cols[i]->data)[k] = d_conv(o->l[j].rdate);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_shipinstruct") == 0){
                 ((char**)t->cols[i]->data)[k] = strdup(o->l[j].shipinstruct);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_shipmode") == 0){
                 ((char**)t->cols[i]->data)[k] = strdup(o->l[j].shipmode);
                 continue;
             }
             if(strcmp(t->cols[i]->name, "l_comment") == 0){
                 ((char**)t->cols[i]->data)[k] = strdup(o->l[j].comment);
                 continue;
             }
             assert(false);
       }
       t->counter++;
    }
}

static void init_tbl(int tnum, DSS_HUGE count, tpch_info_t* info) {

    switch (tnum) {
        case LINE:
        case ORDER:
        case ORDER_LINE:
            init_info(&(info->ORDER_INFO), count);
            init_info(&(info->LINE_INFO), count);
            break;
        case SUPP:
            init_info(&(info->SUPP_INFO), count);
            break;
        case CUST:
            init_info(&(info->CUST_INFO), count);
            break;
        case PSUPP:
        case PART:
        case PART_PSUPP:
            init_info(&(info->PSUPP_INFO), count);
            init_info(&(info->PART_INFO), count);
            break;
        case NATION:
            init_info(&(info->NATION_INFO), count);
            break;
        case REGION:
            init_info(&(info->REGION_INFO), count);
            break;
    }
}

static void gen_tbl(int tnum, DSS_HUGE count, tpch_info_t* info) {
	order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;

	for (DSS_HUGE i = 1; count; count--, i++) {
		row_start(tnum);
		switch (tnum) {
		case LINE:
		case ORDER:
		case ORDER_LINE:
			mk_order(i, &o, 0);
			append_order(&o, &(info->ORDER_INFO));
            // TDOO FIX
			append_line(&o, &(info->LINE_INFO));
			break;
		case SUPP:
			mk_supp(i, &supp);
			append_supp(&supp, &(info->SUPP_INFO));
			break;
		case CUST:
			mk_cust(i, &cust);
			append_cust(&cust, &(info->CUST_INFO));
			break;
		case PSUPP:
		case PART:
		case PART_PSUPP:
			mk_part(i, &part);
            append_part(&part, &(info->PART_INFO));
			append_psupp(&part, &(info->PSUPP_INFO));
			break;
		case NATION:
			mk_nation(i, &code);
			append_nation(&code, &(info->NATION_INFO));
			break;
		case REGION:
			mk_region(i, &code);
			append_region(&code, &(info->REGION_INFO));
			break;
		}
		row_stop_h(tnum);
	}
}

char*  get_table_name(int num) {
	switch (num) {
	case PART:
		return "part";
	case PSUPP:
		return "partsupp";
	case SUPP:
		return "supplier";
	case CUST:
		return "customer";
	case ORDER:
		return "orders";
	case LINE:
		return "lineitem";
	case NATION:
		return "nation";
	case REGION:
		return "region";
	default:
		return "";
	}
}

#define REGION_SCHEMA(schema) "CREATE TABLE "schema".region"\
	       " ("\
	       "r_regionkey INT NOT NULL,"\
	       "r_name VARCHAR(25) NOT NULL,"\
	       "r_comment VARCHAR(152) NOT NULL);"

#define NATION_SCHEMA(schema) "CREATE TABLE "schema".nation"\
	       " ("\
	       "n_nationkey INT NOT NULL,"\
	       "n_name VARCHAR(25) NOT NULL,"\
	       "n_regionkey INT NOT NULL,"\
	       "n_comment VARCHAR(152) NOT NULL);"

#define SUPPLIER_SCHEMA(schema) "CREATE TABLE "schema".supplier"\
	       " ("\
	       "s_suppkey INT NOT NULL,"\
	       "s_name VARCHAR(25) NOT NULL,"\
	       "s_address VARCHAR(40) NOT NULL,"\
	       "s_nationkey INT NOT NULL,"\
	       "s_phone VARCHAR(15) NOT NULL,"\
	       "s_acctbal DECIMAL(15,2) NOT NULL,"\
	       "s_comment VARCHAR(101) NOT NULL);"

#define CUSTOMER_SCHEMA(schema) "CREATE TABLE "schema".customer"\
	       " ("\
	       "c_custkey INT NOT NULL,"\
	       "c_name VARCHAR(25) NOT NULL,"\
	       "c_address VARCHAR(40) NOT NULL,"\
	       "c_nationkey INT NOT NULL,"\
	       "c_phone VARCHAR(15) NOT NULL,"\
	       "c_acctbal DECIMAL(15,2) NOT NULL,"\
	       "c_mktsegment VARCHAR(10) NOT NULL,"\
	       "c_comment VARCHAR(117) NOT NULL);"

#define PART_SCHEMA(schema) "CREATE TABLE "schema".part"\
	       " ("\
	       "p_partkey INT NOT NULL,"\
	       "p_name VARCHAR(55) NOT NULL,"\
	       "p_mfgr VARCHAR(25) NOT NULL,"\
	       "p_brand VARCHAR(10) NOT NULL,"\
	       "p_type VARCHAR(25) NOT NULL,"\
	       "p_size INT NOT NULL,"\
	       "p_container VARCHAR(10) NOT NULL,"\
	       "p_retailprice DECIMAL(15,2) NOT NULL,"\
	       "p_comment VARCHAR(23) NOT NULL);"


#define PART_SUPP_SCHEMA(schema) "CREATE TABLE "schema".partsupp"\
	       " ("\
	       "ps_partkey INT NOT NULL,"\
	       "ps_suppkey INT NOT NULL,"\
	       "ps_availqty INT NOT NULL,"\
	       "ps_supplycost DECIMAL(15,2) NOT NULL,"\
	       "ps_comment VARCHAR(199) NOT NULL);"

#define ORDERS_SCHEMA(schema) "CREATE TABLE "schema".orders"\
	       " ("\
	       "o_orderkey INT NOT NULL,"\
	       "o_custkey INT NOT NULL,"\
	       "o_orderstatus VARCHAR(1) NOT NULL,"\
	       "o_totalprice DECIMAL(15,2) NOT NULL,"\
	       "o_orderdate DATE NOT NULL,"\
	       "o_orderpriority VARCHAR(15) NOT NULL,"\
	       "o_clerk VARCHAR(15) NOT NULL,"\
	       "o_shippriority INT NOT NULL,"\
	       "o_comment VARCHAR(79) NOT NULL);"

#define LINE_ITEM_SCHEMA(schema) "CREATE TABLE "schema".lineitem"\
	       " ("\
	       "l_orderkey INT NOT NULL,"\
	       "l_partkey INT NOT NULL,"\
	       "l_suppkey INT NOT NULL,"\
	       "l_linenumber INT NOT NULL,"\
	       "l_quantity INTEGER NOT NULL,"\
	       "l_extendedprice DECIMAL(15,2) NOT NULL,"\
	       "l_discount DECIMAL(15,2) NOT NULL,"\
	       "l_tax DECIMAL(15,2) NOT NULL,"\
	       "l_returnflag VARCHAR(1) NOT NULL,"\
	       "l_linestatus VARCHAR(1) NOT NULL,"\
	       "l_shipdate DATE NOT NULL,"\
	       "l_commitdate DATE NOT NULL,"\
	       "l_receiptdate DATE NOT NULL,"\
	       "l_shipinstruct VARCHAR(25) NOT NULL,"\
	       "l_shipmode VARCHAR(10) NOT NULL,"\
	       "l_comment VARCHAR(44) NOT NULL)"

char* dbgen(double flt_scale, monetdbe_database mdbe, char* schema){
    char* err= NULL;
    if((err=monetdbe_query(mdbe, REGION_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, NATION_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, SUPPLIER_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, CUSTOMER_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, PART_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, PART_SUPP_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, ORDERS_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
    if((err=monetdbe_query(mdbe, LINE_ITEM_SCHEMA("sys"), NULL, NULL)) != NULL)
        return err;
	if (flt_scale == 0) {
		// schema only
		return NULL;
	}
    
	// generate the actual data
	DSS_HUGE rowcnt = 0;
	DSS_HUGE i;
	// all tables
	table = (1 << CUST) | (1 << SUPP) | (1 << NATION) | (1 << REGION) | (1 << PART_PSUPP) | (1 << ORDER_LINE);
	force = 0;
	insert_segments = 0;
	delete_segments = 0;
	insert_orders_segment = 0;
	insert_lineitem_segment = 0;
	delete_segment = 0;
	verbose = 0;
	set_seeds = 0;
	scale = 1;
	updates = 0;

	// check if it is the first invocation
	if (first_invocation) {
		// store the initial random seed
		memcpy(seed_backup, Seed, sizeof(seed_t) * MAX_STREAM + 1);
		first_invocation = false;
	} else {
		// restore random seeds from backup
		memcpy(Seed, seed_backup, sizeof(seed_t) * MAX_STREAM + 1);
	}
	tdefs[PART].base = 200000;
	tdefs[PSUPP].base = 200000;
	tdefs[SUPP].base = 10000;
	tdefs[CUST].base = 150000;
	tdefs[ORDER].base = 150000 * ORDERS_PER_CUST;
	tdefs[LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[ORDER_LINE].base = 150000 * ORDERS_PER_CUST;
	tdefs[PART_PSUPP].base = 200000;
	tdefs[NATION].base = NATIONS_MAX;
	tdefs[REGION].base = NATIONS_MAX;

	children = 1;
	d_path = NULL;
	
    if (flt_scale < MIN_SCALE) {
		int i;
		int int_scale;

		scale = 1;
		int_scale = (int)(1000 * flt_scale);
		for (i = PART; i < REGION; i++) {
			tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base) / 1000;
			if (tdefs[i].base < 1)
				tdefs[i].base = 1;
		}
	} else {
		scale = (long)flt_scale;
	}

	load_dists();
	
    /* have to do this after init */
	tdefs[NATION].base = nations.count;
	tdefs[REGION].base = regions.count;

	/**
	** region_append_info
	**/
    struct append_info_t region_info = {3, NULL, 0, false};
    monetdbe_column* region_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    // monetdbe_column* region_cols[3];

    region_cols[0]->type = monetdbe_int64_t;
    region_cols[1]->type = monetdbe_str;
    region_cols[2]->type = monetdbe_str;
    region_cols[0]->name = "r_regionkey";
    region_cols[1]->name = "r_name";
    region_cols[2]->name = "r_comment";
    region_info.cols = region_cols;
	/**
	** nation_append_info
	**/
    struct append_info_t nation_info = {4, NULL, 0, false};
    monetdbe_column* nation_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    nation_cols[0]->type = monetdbe_int64_t;
    nation_cols[1]->type = monetdbe_str;
    nation_cols[2]->type = monetdbe_int64_t;
    nation_cols[3]->type = monetdbe_str;
    nation_cols[0]->name = "n_nationkey";
    nation_cols[1]->name = "n_name";
    nation_cols[2]->name = "n_regionkey";
    nation_cols[3]->name = "n_comment";
    nation_info.cols = nation_cols;
	/**
	**supplier_append_info
	**/
    struct append_info_t supplier_info = {7, NULL, 0, false};
    monetdbe_column* supplier_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    supplier_cols[0]->type = monetdbe_int64_t;
    supplier_cols[1]->type = monetdbe_str;
    supplier_cols[2]->type = monetdbe_str;
    supplier_cols[3]->type = monetdbe_int64_t;
    supplier_cols[4]->type = monetdbe_str;
    supplier_cols[5]->type = monetdbe_double;
    supplier_cols[6]->type = monetdbe_str;
    supplier_cols[0]->name = "s_suppkey";
    supplier_cols[1]->name = "s_name";
    supplier_cols[2]->name = "s_address";
    supplier_cols[3]->name = "s_nationkey";
    supplier_cols[4]->name = "s_phone";
    supplier_cols[5]->name = "s_acctbal";
    supplier_cols[6]->name = "s_comment";
    supplier_info.cols = supplier_cols;
    
	/**
	**supplier_append_info
	**/
    struct append_info_t customer_info = {8, NULL, 0, false};
    monetdbe_column* customer_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    customer_cols[0]->type = monetdbe_int64_t;
    customer_cols[1]->type = monetdbe_str;
    customer_cols[2]->type = monetdbe_str;
    customer_cols[3]->type = monetdbe_int64_t;
    customer_cols[4]->type = monetdbe_str;
    customer_cols[5]->type = monetdbe_double;
    customer_cols[6]->type = monetdbe_str;
    customer_cols[7]->type = monetdbe_str;
    customer_cols[0]->name ="c_custkey"; 
    customer_cols[1]->name ="c_name"; 
    customer_cols[2]->name ="c_address"; 
    customer_cols[3]->name ="c_nationkey"; 
    customer_cols[4]->name ="c_phone"; 
    customer_cols[5]->name ="c_acctbal"; 
    customer_cols[6]->name ="c_mktsegment"; 
    customer_cols[7]->name ="c_comment"; 
    customer_info.cols = customer_cols;

	/**
	**part_append_info
	**/
    struct append_info_t part_info = {9, NULL, 0, false};
    monetdbe_column* part_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    part_cols[0]->type = monetdbe_int64_t;
    part_cols[1]->type = monetdbe_str;
    part_cols[2]->type = monetdbe_str;
    part_cols[3]->type = monetdbe_str;
    part_cols[4]->type = monetdbe_str;
    part_cols[5]->type = monetdbe_int64_t;
    part_cols[6]->type = monetdbe_str;
    part_cols[7]->type = monetdbe_double;
    part_cols[8]->type = monetdbe_str;
    part_cols[0]->name = "p_partkey";  
    part_cols[1]->name = "p_name"; 
    part_cols[2]->name = "p_mfgr"; 
    part_cols[3]->name = "p_brand"; 
    part_cols[4]->name = "p_type"; 
    part_cols[5]->name = "p_size"; 
    part_cols[6]->name = "p_container"; 
    part_cols[7]->name = "p_retailprice"; 
    part_cols[8]->name = "p_comment"; 
    part_info.cols = part_cols;


	/**
	**psupp_append_info
	**/
    struct append_info_t psupp_info = {5, NULL, 0, false};
    monetdbe_column* psupp_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    psupp_cols[0]->type = monetdbe_int64_t;
    psupp_cols[1]->type = monetdbe_int64_t;
    psupp_cols[2]->type = monetdbe_int64_t;
    psupp_cols[3]->type = monetdbe_double;
    psupp_cols[4]->type = monetdbe_str;
    psupp_cols[0]->name = "ps_partkey";
    psupp_cols[1]->name = "ps_suppkey";
    psupp_cols[2]->name = "ps_availqty";
    psupp_cols[3]->name = "ps_supplycost";
    psupp_cols[4]->name = "ps_comment";
    psupp_info.cols = psupp_cols;

	/**
	**orders_append_info
	**/
    struct append_info_t orders_info = {9, NULL, 0, false};
    monetdbe_column* orders_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_date)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    orders_cols[0]->type = monetdbe_int64_t;
    orders_cols[1]->type = monetdbe_int64_t;
    orders_cols[2]->type = monetdbe_str;
    orders_cols[3]->type = monetdbe_double;
    orders_cols[4]->type = monetdbe_date;
    orders_cols[5]->type = monetdbe_str;
    orders_cols[6]->type = monetdbe_str;
    orders_cols[7]->type = monetdbe_int64_t;
    orders_cols[8]->type = monetdbe_str;
    orders_cols[0]->name = "o_orderkey";
    orders_cols[1]->name = "o_custkey";
    orders_cols[2]->name = "o_orderstatus";
    orders_cols[3]->name = "o_totalprice";
    orders_cols[4]->name = "o_orderdate";
    orders_cols[5]->name = "o_orderpriority";
    orders_cols[6]->name = "o_clerk";
    orders_cols[7]->name = "o_shippriority";
    orders_cols[8]->name = "o_comment";
    orders_info.cols = orders_cols;

	/**
	**lineitem_append_info
	**/
    struct append_info_t lineitem_info = {16, NULL, 0, false};
    monetdbe_column* lineitem_cols[] = {
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_int64_t)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_double)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_date)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_date)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_date)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str)),
        (monetdbe_column*) malloc(sizeof(monetdbe_column_str))
    };
    lineitem_cols[0]->type = monetdbe_int64_t;
    lineitem_cols[1]->type = monetdbe_int64_t;
    lineitem_cols[2]->type = monetdbe_int64_t;
    lineitem_cols[3]->type = monetdbe_int64_t;
    lineitem_cols[4]->type = monetdbe_int64_t;
    lineitem_cols[5]->type = monetdbe_double; 
    lineitem_cols[6]->type = monetdbe_double; 
    lineitem_cols[7]->type = monetdbe_double; 
    lineitem_cols[8]->type = monetdbe_str;
    lineitem_cols[9]->type = monetdbe_str;
    lineitem_cols[10]->type = monetdbe_date;
    lineitem_cols[11]->type = monetdbe_date;
    lineitem_cols[12]->type = monetdbe_date;
    lineitem_cols[13]->type = monetdbe_str;
    lineitem_cols[14]->type = monetdbe_str;
    lineitem_cols[15]->type = monetdbe_str;
    lineitem_cols[0]->name = "l_orderkey";
    lineitem_cols[1]->name = "l_partkey";
    lineitem_cols[2]->name = "l_suppkey";
    lineitem_cols[3]->name = "l_linenumber";
    lineitem_cols[4]->name = "l_quantity";
    lineitem_cols[5]->name = "l_extendedprice";
    lineitem_cols[6]->name = "l_discount";
    lineitem_cols[7]->name = "l_tax";
    lineitem_cols[8]->name = "l_returnflag";
    lineitem_cols[9]->name = "l_linestatus";
    lineitem_cols[10]->name = "l_shipdate";
    lineitem_cols[11]->name = "l_commitdate";
    lineitem_cols[12]->name = "l_receiptdate";
    lineitem_cols[13]->name = "l_shipinstruct";
    lineitem_cols[14]->name = "l_shipmode";
    lineitem_cols[15]->name = "l_comment";
    lineitem_info.cols = lineitem_cols;

    struct tpch_info_t tpch_info = {
        .PART_INFO=part_info, 
        .PSUPP_INFO=psupp_info, 
        .SUPP_INFO=supplier_info, 
        .CUST_INFO=customer_info, 
        .ORDER_INFO=orders_info,
        .LINE_INFO=lineitem_info,
        .NATION_INFO=nation_info,
        .REGION_INFO=region_info
    };

    for (i = PART; i <= REGION; i++) {
		if (table & (1 << i)) {
			if (i < NATION) {
				rowcnt = tdefs[i].base * scale;
			} else {
				rowcnt = tdefs[i].base;
			}
			// actually doing something
            // rowcnt=10;
            printf("%s, rowcount=%d\n", get_table_name(i), rowcnt);
            printf("---------------\n");
			init_tbl((int)i, rowcnt, &tpch_info);
			gen_tbl((int)i, rowcnt, &tpch_info);
		}
	}
    printf("BEGIN APPEND ...\n");
    //if ((err = monetdbe_append(mdbe, "sys", "region", tpch_info.REGION_INFO.cols, tpch_info.REGION_INFO.ncols)) != NULL)
    //    return err;
  
    //if ((err = monetdbe_append(mdbe, "sys", "nation", tpch_info.NATION_INFO.cols, tpch_info.NATION_INFO.ncols)) != NULL)
    //   return err;

    //if ((err = monetdbe_append(mdbe, "sys", "customer", tpch_info.CUST_INFO.cols, tpch_info.CUST_INFO.ncols)) != NULL)
    //    return err;

    //if ((err = monetdbe_append(mdbe, "sys", "supplier", tpch_info.SUPP_INFO.cols, tpch_info.SUPP_INFO.ncols)) != NULL)
    //    return err;
    if ((err = monetdbe_append(mdbe, "sys", "part", tpch_info.PART_INFO.cols, tpch_info.PART_INFO.ncols)) != NULL)
        return err;
    if ((err = monetdbe_append(mdbe, "sys", "partsupp", tpch_info.PSUPP_INFO.cols, tpch_info.PSUPP_INFO.ncols)) != NULL)
        return err;
    //if ((err = monetdbe_append(mdbe, "sys", "orders", tpch_info.ORDER_INFO.cols, tpch_info.ORDER_INFO.ncols)) != NULL)
    //    return err;
//    if ((err = monetdbe_append(mdbe, "sys", "lineitem", tpch_info.LINE_INFO.cols, tpch_info.LINE_INFO.ncols)) != NULL)
//        return err;


    return NULL;
}


