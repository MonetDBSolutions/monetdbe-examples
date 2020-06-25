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


/*
* Function prototypes
*/
void	kill_load (void);
int		pload (int tbl);
void	gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num);
int		pr_drange (int tbl, DSS_HUGE min, DSS_HUGE cnt, long num);
int		set_files (int t, int pload);
int		partial (int, int);


extern int optind, opterr;
extern char *optarg;
DSS_HUGE rowcnt = 0, minrow = 0;
long upd_num = 0;
double flt_scale;
#if (defined(WIN32)&&!defined(_POSIX_))
char *spawn_args[25];
#endif
#ifdef RNG_TEST
extern seed_t Seed[];
#endif
static int bTableSet = 0;


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

//monetdbe_export char* monetdbe_append(monetdbe_database dbhdl, const char* schema, const char* table, monetdbe_column **input, size_t column_count);
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
* re-set default output file names 
*/
int
set_files (int i, int pload)
{
	char line[80], *new_name;
	
	if (table & (1 << i))
child_table:
	{
		if (pload != -1)
			sprintf (line, "%s.%d", tdefs[i].name, pload);
		else
		{
			printf ("Enter new destination for %s data: ",
				tdefs[i].name);
			if (fgets (line, sizeof (line), stdin) == NULL)
				return (-1);;
			if ((new_name = strchr (line, '\n')) != NULL)
				*new_name = '\0';
			if ((int)strlen (line) == 0)
				return (0);
		}
		new_name = (char *) malloc ((int)strlen (line) + 1);
		MALLOC_CHECK (new_name);
		strcpy (new_name, line);
		tdefs[i].name = new_name;
		if (tdefs[i].child != NONE)
		{
			i = tdefs[i].child;
			tdefs[i].child = NONE;
			goto child_table;
		}
	}
	
	return (0);
}



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

/*
* generate a particular table
*/
void
gen_tbl (int tnum, DSS_HUGE start, DSS_HUGE count, long upd_num)
{
	static order_t o;
	supplier_t supp;
	customer_t cust;
	part_t part;
	code_t code;
	static int completed = 0;
	DSS_HUGE i;

	DSS_HUGE rows_per_segment=0;
	DSS_HUGE rows_this_segment=-1;
	DSS_HUGE residual_rows=0;

	if (insert_segments)
		{
		rows_per_segment = count / insert_segments;
		residual_rows = count - (rows_per_segment * insert_segments);
		}

	for (i = start; count; count--, i++)
	{
		LIFENOISE (1000, i);
		row_start(tnum);

		switch (tnum)
		{
		case LINE:
		case ORDER:
  		case ORDER_LINE: 
			mk_order (i, &o, upd_num % 10000);

		  if (insert_segments  && (upd_num > 0))
			if((upd_num / 10000) < residual_rows)
				{
				if((++rows_this_segment) > rows_per_segment) 
					{						
					rows_this_segment=0;
					upd_num += 10000;					
					}
				}
			else
				{
				if((++rows_this_segment) >= rows_per_segment) 
					{
					rows_this_segment=0;
					upd_num += 10000;
					}
				}

			if (set_seeds == 0)
				tdefs[tnum].loader(&o, upd_num);
			break;
		case SUPP:
			mk_supp (i, &supp);
			if (set_seeds == 0)
				tdefs[tnum].loader(&supp, upd_num);
			break;
		case CUST:
			mk_cust (i, &cust);
			if (set_seeds == 0)
				tdefs[tnum].loader(&cust, upd_num);
			break;
		case PSUPP:
		case PART:
  		case PART_PSUPP: 
			mk_part (i, &part);
			if (set_seeds == 0)
				tdefs[tnum].loader(&part, upd_num);
			break;
		case NATION:
			mk_nation (i, &code);
			if (set_seeds == 0)
				tdefs[tnum].loader(&code, 0);
			break;
		case REGION:
			mk_region (i, &code);
			if (set_seeds == 0)
				tdefs[tnum].loader(&code, 0);
			break;
		}
		row_stop(tnum);
		if (set_seeds && (i % tdefs[tnum].base) < 2)
		{
			printf("\nSeeds for %s at rowcount %ld\n", tdefs[tnum].comment, i);
			dump_seeds(tnum);
		}
	}
	completed |= 1 << tnum;
}



/*
* int partial(int tbl, int s) -- generate the s-th part of the named tables data
*/
int
partial (int tbl, int s)
{
	DSS_HUGE rowcnt;
	DSS_HUGE extra;
	
	if (verbose > 0)
	{
		fprintf (stderr, "\tStarting to load stage %d of %d for %s...",
			s, children, tdefs[tbl].comment);
	}
	
	set_files (tbl, s);
	
	rowcnt = set_state(tbl, scale, children, s, &extra);

	if (s == children)
		gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt + extra, upd_num);
	else
		gen_tbl (tbl, rowcnt * (s - 1) + 1, rowcnt, upd_num);
	
	if (verbose > 0)
		fprintf (stderr, "done.\n");
	
	return (0);
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
    return NULL;
}


/*
* MAIN
*
* assumes the existance of getopt() to clean up the command 
* line handling
*/
//int
//main (int ac, char **av)
//{
//	DSS_HUGE i;
//	
//	table = (1 << CUST) |
//		(1 << SUPP) |
//		(1 << NATION) |
//		(1 << REGION) |
//		(1 << PART_PSUPP) |
//		(1 << ORDER_LINE);
//	force = 0;
//    insert_segments=0;
//    delete_segments=0;
//    insert_orders_segment=0;
//    insert_lineitem_segment=0;
//    delete_segment=0;
//	verbose = 0;
//	set_seeds = 0;
//	scale = 1;
//	flt_scale = 1.0;
//	updates = 0;
//	step = -1;
//	tdefs[ORDER].base *=
//		ORDERS_PER_CUST;			/* have to do this after init */
//	tdefs[LINE].base *=
//		ORDERS_PER_CUST;			/* have to do this after init */
//	tdefs[ORDER_LINE].base *=
//		ORDERS_PER_CUST;			/* have to do this after init */
//	children = 1;
//	d_path = NULL;
//	
//#ifdef NO_SUPPORT
//	signal (SIGINT, exit);
//#endif /* NO_SUPPORT */
//#if (defined(WIN32)&&!defined(_POSIX_))
//	for (i = 0; i < ac; i++)
//	{
//		spawn_args[i] = malloc (((int)strlen (av[i]) + 1) * sizeof (char));
//		MALLOC_CHECK (spawn_args[i]);
//		strcpy (spawn_args[i], av[i]);
//	}
//	spawn_args[ac] = NULL;
//#endif
//	
//	if (verbose >= 0)
//		{
//		fprintf (stderr,
//			"%s Population Generator (Version %d.%d.%d)\n",
//			NAME, VERSION, RELEASE, PATCH);
//		fprintf (stderr, "Copyright %s %s\n", TPC, C_DATES);
//		}
//	
//	load_dists ();
//#ifdef RNG_TEST
//	for (i=0; i <= MAX_STREAM; i++)
//		Seed[i].nCalls = 0;
//#endif
//	/* have to do this after init */
//	tdefs[NATION].base = nations.count;
//	tdefs[REGION].base = regions.count;
//	
//	/* 
//	* updates are never parallelized 
//	*/
//	if (updates)
//		{
//		/* 
//		 * set RNG to start generating rows beyond SF=scale
//		 */
//		set_state (ORDER, scale, 100, 101, &i); 
//		rowcnt = (int)(tdefs[ORDER_LINE].base / 10000 * scale * UPD_PCT);
//		if (step > 0)
//			{
//			/* 
//			 * adjust RNG for any prior update generation
//			 */
//	      for (i=1; i < step; i++)
//         {
//			sd_order(0, rowcnt);
//			sd_line(0, rowcnt);
//         }
//			upd_num = step - 1;
//			}
//		else
//			upd_num = 0;
//
//		while (upd_num < updates)
//			{
//			if (verbose > 0)
//				fprintf (stderr,
//				"Generating update pair #%d for %s",
//				upd_num + 1, tdefs[ORDER_LINE].comment);
//			insert_orders_segment=0;
//			insert_lineitem_segment=0;
//			delete_segment=0;
//			minrow = upd_num * rowcnt + 1;
//			gen_tbl (ORDER_LINE, minrow, rowcnt, upd_num + 1);
//			if (verbose > 0)
//				fprintf (stderr, "done.\n");
//			pr_drange (ORDER_LINE, minrow, rowcnt, upd_num + 1);
//			upd_num++;
//			}
//
//		exit (0);
//		}
//	
//	/**
//	** actual data generation section starts here
//	**/
//
//	/*
//	* traverse the tables, invoking the appropriate data generation routine for any to be built
//	*/
//	for (i = PART; i <= REGION; i++)
//		if (table & (1 << i))
//		{
//			if (children > 1 && i < NATION)
//			{
//				partial ((int)i, step);
//			}
//			else
//			{
//				minrow = 1;
//				if (i < NATION)
//					rowcnt = tdefs[i].base * scale;
//				else
//					rowcnt = tdefs[i].base;
//				if (verbose > 0)
//					fprintf (stderr, "Generating data for %s", tdefs[i].comment);
//				gen_tbl ((int)i, minrow, rowcnt, upd_num);
//				if (verbose > 0)
//					fprintf (stderr, "done.\n");
//			}
//		}
//			
//		return (0);
//}
