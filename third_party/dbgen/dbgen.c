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
#include "tpch_constants.h"

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
static void clean_up(tpch_info_t*);

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
            return malloc(sizeof(char*)*n);
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

//static void* append_str(char* buff, const char* str) {
//    size_t buff_length = (buff == NULL) ? 0 : strlen(buff);
//    size_t str_length = strlen(str) + 1;
//    size_t out_length = buff_length + str_length;
//    char out[out_length];
//    memcpy(out, buff, buff_length);
//    return memcpy(out + buff_length, str, str_length + 1);
//}

static void append_region(code_t* c, append_info_t* t) {
    size_t k = t->counter;
    for (size_t i=0; i < (t->ncols); i++) {
        if(strcmp(t->cols[i]->name, "r_regionkey") == 0){
            ((int64_t*)t->cols[i]->data)[k] = c->code; 
            continue;
        }
        if(strcmp(t->cols[i]->name, "r_name") == 0){
            char* name = strdup(c->text);
            if (name != NULL)
                ((char**)t->cols[i]->data)[k] = name; 
            continue;
        }
        if(strcmp(t->cols[i]->name, "r_comment") == 0){
            char* comment = strdup(c->comment);
            if (comment != NULL)
                ((char**)t->cols[i]->data)[k] = comment; 
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
    size_t check = SUPP_PER_PART;
    assert((sizeof(p->s)/sizeof(partsupp_t)) == SUPP_PER_PART);
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
                ((char**)t->cols[i]->data)[k] = strdup(p->s[j].comment);
                continue;
            }
            assert(false);
        }
        k++;
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

static char* char_to_str(char c){
    char res[2];
    res[0]=c;
    res[1]='\0'; 
    return strdup(res);
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
            char* orderstatus = char_to_str(o->orderstatus);
            ((char**)t->cols[i]->data)[k] = orderstatus; 
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
                ((char**)t->cols[i]->data)[k] = char_to_str((o->l[j].rflag[0]));
                continue;
            }
            if(strcmp(t->cols[i]->name, "l_linestatus") == 0){
                ((char**)t->cols[i]->data)[k] = char_to_str(o->l[j].lstatus[0]);
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
        k++;
        t->counter++;
    }
}

static void init_tbl(int tnum, DSS_HUGE count, tpch_info_t* info) {

    switch (tnum) {
        case LINE:
        case ORDER:
        case ORDER_LINE:
            init_info(&(info->ORDER_INFO), count);
            init_info(&(info->LINE_INFO), count*O_LCNT_MAX);
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
            init_info(&(info->PART_INFO), count);
            init_info(&(info->PSUPP_INFO), count*SUPP_PER_PART);
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
    clock_t begin_gen = clock();
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
    monetdbe_column region_col0 = { .type = monetdbe_int64_t, .name = "r_regionkey" };
    monetdbe_column region_col1 = { .type = monetdbe_str, .name = "r_name" };
    monetdbe_column region_col2 = { .type = monetdbe_str, .name = "r_comment" };
    monetdbe_column* region_cols[3] = { &region_col0, &region_col1, &region_col2 };
    struct append_info_t region_info = { .ncols = 3, .cols = (monetdbe_column**) &region_cols, .counter = 0, .init = false};


    /**
     ** nation_append_info
     **/
    monetdbe_column nation_col0 = { .type = monetdbe_int64_t, .name = "n_nationkey" };
    monetdbe_column nation_col1 = { .type = monetdbe_str, .name = "n_name" };
    monetdbe_column nation_col2 = { .type = monetdbe_int64_t, .name = "n_regionkey" };
    monetdbe_column nation_col3 = { .type = monetdbe_str, .name = "n_comment" };
    monetdbe_column* nation_cols[4] = { &nation_col0, &nation_col1, &nation_col2, &nation_col3 };
    struct append_info_t nation_info = { .ncols = 4, .cols = (monetdbe_column**) &nation_cols, .counter = 0, .init = false};


    /**
     ** supplier_append_info
     **/
    monetdbe_column supplier_col0 = { .type = monetdbe_int64_t, .name = "s_suppkey" };
    monetdbe_column supplier_col1 = { .type = monetdbe_str, .name = "s_name" };
    monetdbe_column supplier_col2 = { .type = monetdbe_str, .name = "s_address" };
    monetdbe_column supplier_col3 = { .type = monetdbe_int64_t, .name = "s_nationkey" };
    monetdbe_column supplier_col4 = { .type = monetdbe_str, .name = "s_phone" };
    monetdbe_column supplier_col5 = { .type = monetdbe_double, .name = "s_acctbal" };
    monetdbe_column supplier_col6 = { .type = monetdbe_str, .name = "s_comment" };
    monetdbe_column* supplier_cols[7] = { &supplier_col0, &supplier_col1, &supplier_col2, &supplier_col3, &supplier_col4, &supplier_col5, &supplier_col6 };
    struct append_info_t supplier_info = { .ncols = 7, .cols = (monetdbe_column**) &supplier_cols, .counter = 0, .init = false};


    /**
     ** customer_append_info
     **/
    monetdbe_column customer_col0 = { .type = monetdbe_int64_t, .name = "c_custkey" };
    monetdbe_column customer_col1 = { .type = monetdbe_str, .name = "c_name" };
    monetdbe_column customer_col2 = { .type = monetdbe_str, .name = "c_address" };
    monetdbe_column customer_col3 = { .type = monetdbe_int64_t, .name = "c_nationkey" };
    monetdbe_column customer_col4 = { .type = monetdbe_str, .name = "c_phone" };
    monetdbe_column customer_col5 = { .type = monetdbe_double, .name = "c_acctbal" };
    monetdbe_column customer_col6 = { .type = monetdbe_str, .name = "c_mktsegment" };
    monetdbe_column customer_col7 = { .type = monetdbe_str, .name = "c_comment" };
    monetdbe_column* customer_cols[8] = { &customer_col0, &customer_col1, &customer_col2, &customer_col3, 
        &customer_col4, &customer_col5, &customer_col6, &customer_col7 };
    struct append_info_t customer_info = { .ncols = 8, .cols = (monetdbe_column**) &customer_cols, .counter = 0, .init = false};


    /**
     ** part_append_info
     **/
    monetdbe_column part_col0 = { .type = monetdbe_int64_t, .name = "p_partkey" };
    monetdbe_column part_col1 = { .type = monetdbe_str, .name = "p_name" };
    monetdbe_column part_col2 = { .type = monetdbe_str, .name = "p_mfgr" };
    monetdbe_column part_col3 = { .type = monetdbe_str, .name = "p_brand" };
    monetdbe_column part_col4 = { .type = monetdbe_str, .name = "p_type" };
    monetdbe_column part_col5 = { .type = monetdbe_int64_t, .name = "p_size" };
    monetdbe_column part_col6 = { .type = monetdbe_str, .name = "p_container" };
    monetdbe_column part_col7 = { .type = monetdbe_double, .name = "p_retailprice" };
    monetdbe_column part_col8 = { .type = monetdbe_str, .name = "p_comment" };
    monetdbe_column* part_cols[9] = { &part_col0, &part_col1, &part_col2, &part_col3, &part_col4, &part_col5, &part_col6, &part_col7, &part_col8 };
    struct append_info_t part_info = { .ncols = 9, .cols = (monetdbe_column**) &part_cols, .counter = 0, .init = false};


    /**
     ** psupp_append_info
     **/
    monetdbe_column psupp_col0 = { .type = monetdbe_int64_t, .name = "ps_partkey" };
    monetdbe_column psupp_col1 = { .type = monetdbe_int64_t, .name = "ps_suppkey" };
    monetdbe_column psupp_col2 = { .type = monetdbe_int64_t, .name = "ps_availqty" };
    monetdbe_column psupp_col3 = { .type = monetdbe_double, .name = "ps_supplycost" };
    monetdbe_column psupp_col4 = { .type = monetdbe_str, .name = "ps_comment" };
    monetdbe_column* psupp_cols[5] = { &psupp_col0, &psupp_col1, &psupp_col2, &psupp_col3, &psupp_col4 };
    struct append_info_t psupp_info = { .ncols = 5, .cols = (monetdbe_column**) &psupp_cols, .counter = 0, .init = false};


    /**
     **  orders_append_info
     **/
    monetdbe_column orders_col0 = { .type = monetdbe_int64_t, .name = "o_orderkey" };
    monetdbe_column orders_col1 = { .type = monetdbe_int64_t, .name = "o_custkey" };
    monetdbe_column orders_col2 = { .type = monetdbe_str, .name = "o_orderstatus" };
    monetdbe_column orders_col3 = { .type = monetdbe_double, .name = "o_totalprice" };
    monetdbe_column orders_col4 = { .type = monetdbe_date, .name = "o_orderdate" };
    monetdbe_column orders_col5 = { .type = monetdbe_str, .name = "o_orderpriority" };
    monetdbe_column orders_col6 = { .type = monetdbe_str, .name = "o_clerk" };
    monetdbe_column orders_col7 = { .type = monetdbe_int64_t, .name = "o_shippriority" };
    monetdbe_column orders_col8 = { .type = monetdbe_str, .name = "o_comment" };
    monetdbe_column* orders_cols[9] = { &orders_col0, &orders_col1, &orders_col2, &orders_col3, &orders_col4, &orders_col5, 
        &orders_col6, &orders_col7, &orders_col8 };
    struct append_info_t orders_info = { .ncols = 9, .cols = (monetdbe_column**) &orders_cols, .counter = 0, .init = false};

    /**
     ** lineitem_append_info
     **/
    monetdbe_column lineitem_col0 = { .type = monetdbe_int64_t, .name = "l_orderkey" };
    monetdbe_column lineitem_col1 = { .type = monetdbe_int64_t, .name = "l_partkey" };
    monetdbe_column lineitem_col2 = { .type = monetdbe_int64_t, .name = "l_suppkey" };
    monetdbe_column lineitem_col3 = { .type = monetdbe_int64_t, .name = "l_linenumber" };
    monetdbe_column lineitem_col4 = { .type = monetdbe_int64_t, .name = "l_quantity" };
    monetdbe_column lineitem_col5 = { .type = monetdbe_double, .name = "l_extendedprice" };
    monetdbe_column lineitem_col6 = { .type = monetdbe_double, .name = "l_discount" };
    monetdbe_column lineitem_col7 = { .type = monetdbe_double, .name = "l_tax" };
    monetdbe_column lineitem_col8 = { .type = monetdbe_str, .name = "l_returnflag" };
    monetdbe_column lineitem_col9 = { .type = monetdbe_str, .name = "l_linestatus" };
    monetdbe_column lineitem_col10 = { .type = monetdbe_date, .name = "l_shipdate" };
    monetdbe_column lineitem_col11 = { .type = monetdbe_date, .name = "l_commitdate" };
    monetdbe_column lineitem_col12 = { .type = monetdbe_date, .name = "l_receiptdate" };
    monetdbe_column lineitem_col13 = { .type = monetdbe_str, .name = "l_shipinstruct" };
    monetdbe_column lineitem_col14 = { .type = monetdbe_str, .name = "l_shipmode" };
    monetdbe_column lineitem_col15 = { .type = monetdbe_str, .name = "l_comment" };
    monetdbe_column* lineitem_cols[16] = { &lineitem_col0, &lineitem_col1, &lineitem_col2, &lineitem_col3, &lineitem_col4, 
        &lineitem_col5, &lineitem_col6, &lineitem_col7, &lineitem_col8, &lineitem_col9, &lineitem_col10,
        &lineitem_col11, &lineitem_col12, &lineitem_col13, &lineitem_col14, &lineitem_col15 };
    struct append_info_t lineitem_info = { .ncols = 16, .cols = (monetdbe_column**) &lineitem_cols, .counter = 0, .init = false};


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

    clock_t end_gen = clock();
    printf("BEGIN APPEND ...\n");
    if ((err = monetdbe_append(mdbe, "sys", "region", tpch_info.REGION_INFO.cols, tpch_info.REGION_INFO.ncols)) != NULL)
        return err;

    if ((err = monetdbe_append(mdbe, "sys", "nation", tpch_info.NATION_INFO.cols, tpch_info.NATION_INFO.ncols)) != NULL)
        return err;

    if ((err = monetdbe_append(mdbe, "sys", "customer", tpch_info.CUST_INFO.cols, tpch_info.CUST_INFO.ncols)) != NULL)
        return err;

    if ((err = monetdbe_append(mdbe, "sys", "supplier", tpch_info.SUPP_INFO.cols, tpch_info.SUPP_INFO.ncols)) != NULL)
        return err;
    if ((err = monetdbe_append(mdbe, "sys", "part", tpch_info.PART_INFO.cols, tpch_info.PART_INFO.ncols)) != NULL)
        return err;
    if ((err = monetdbe_append(mdbe, "sys", "partsupp", tpch_info.PSUPP_INFO.cols, tpch_info.PSUPP_INFO.ncols)) != NULL)
        return err;
    if ((err = monetdbe_append(mdbe, "sys", "orders", tpch_info.ORDER_INFO.cols, tpch_info.ORDER_INFO.ncols)) != NULL)
        return err;
    if ((err = monetdbe_append(mdbe, "sys", "lineitem", tpch_info.LINE_INFO.cols, tpch_info.LINE_INFO.ncols)) != NULL)
        return err;

    clock_t end_load = clock();
    double time_spent=0.0;
    printf("appending took %f sec\n\n", (double)(end_load - end_gen)/CLOCKS_PER_SEC);
    clean_up(&tpch_info);
    return NULL;
}

static void free_data(append_info_t *info){
    for (size_t i=0; i < info->ncols; i++) {
        free(info->cols[i]->data);
    }
}

static void cleanup_dist(distribution *target) {
	if (!target) {
		return;
	}
	if (target->list) {
		for (int i = 0; i < target->count; i++) {
			if (target->list[i].text) {
				free(target->list[i].text);
			}
		}
		free(target->list);
	}
}

void cleanup_dists(void) {
	cleanup_dist(&p_cntr_set);
	cleanup_dist(&colors);
	cleanup_dist(&p_types_set);
	cleanup_dist(&nations);
	cleanup_dist(&regions);
	cleanup_dist(&o_priority_set);
	cleanup_dist(&l_instruct_set);
	cleanup_dist(&l_smode_set);
	cleanup_dist(&l_category_set);
	cleanup_dist(&l_rflag_set);
	cleanup_dist(&c_mseg_set);
	cleanup_dist(&nouns);
	cleanup_dist(&verbs);
	cleanup_dist(&adjectives);
	cleanup_dist(&adverbs);
	cleanup_dist(&auxillaries);
	cleanup_dist(&terminators);
	cleanup_dist(&articles);
	cleanup_dist(&prepositions);
	cleanup_dist(&grammar);
	cleanup_dist(&np);
	cleanup_dist(&vp);
}

static void clean_up(tpch_info_t* info){
    free_data(&(info->REGION_INFO));
    free_data(&(info->NATION_INFO));
    free_data(&(info->CUST_INFO));
    free_data(&(info->SUPP_INFO));
    free_data(&(info->PART_INFO));
    free_data(&(info->PSUPP_INFO));
    free_data(&(info->ORDER_INFO));
    free_data(&(info->LINE_INFO));
    cleanup_dists();
}

const char* get_query(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
	    return NULL;
    }
	return TPCH_QUERIES[query - 1];
}

const char* get_answer(int query) {
	if (query <= 0 || query > TPCH_QUERIES_COUNT) {
	    return NULL;
    }
	return TPCH_ANSWERS_SF1[query - 1];
}

