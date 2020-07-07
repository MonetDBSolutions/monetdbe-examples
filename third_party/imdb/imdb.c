#include <limits.h>
#include <stdlib.h>
#include <unistd.h>
#include "imdb.h"
#include "imdb_constants.h"

static char* copy_into_stmt(char* tbl_name, char* data_file){
    char buf[strlen(tbl_name)+ strlen(data_file) + 100];
    sprintf(buf, "COPY INTO \"%s\" FROM '%s' USING DELIMITERS ',' NULL as '' BEST EFFORT;", tbl_name, data_file);
    return strdup(buf);
}

static char* data_file_path(char* data_path, char* tbl_name){
    char buf[PATH_MAX + strlen(tbl_name)];
    sprintf(buf, "%s/%s.csv.gz", data_path, tbl_name);
    return strdup(buf);
}

char* imdbgen(monetdbe_database mdbe, char* data_dir){
    char data_path[PATH_MAX + 1];
    char *data_file, *q;
    const char *tbl_name, *tbl_ddl;
    char* err = NULL;
    realpath(data_dir, data_path);
    
    for(size_t t=0; t < IMDB_TABLE_COUNT; t++){
        // schema
        tbl_ddl = IMDB_TABLE_DDL[t]; 
        tbl_name = IMDB_TABLE_NAMES[t];
        if((err=monetdbe_query(mdbe,(char*) tbl_ddl, NULL, NULL)) != NULL)
           return err; 
        data_file = data_file_path(data_path,(char*) tbl_name); 
        if(access(data_file, F_OK) != 0) {
            sprintf(err, "Error: file %s does not exists!", data_file);
            return err; 
        }
        q = copy_into_stmt((char*) tbl_name, data_file);
        if((err=monetdbe_query(mdbe,(char*) q, NULL, NULL)) != NULL)
           return err; 

    }

    return NULL;
}

const char* get_query(int query) {
	if (query <= 0 || query > IMDB_QUERIES_COUNT) {
        return NULL;
	}
	return IMDB_QUERIES[query - 1];
}

