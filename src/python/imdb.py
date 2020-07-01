"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

this example is based on https://team-mayes.github.io/che_696/html/notebooks/lecture17_databases.html
This program is intended to load the imdb database sample 
The DDL contains reserved words that need to be quoted.
Awaiting the auto-quoting option in monetdbe-python

 For an explanation of the command arguments see the documentation
 https://monetdbe.readthedocs.io/en/latest/introduction.html
"""
import monetdbe
import json
import time 
import sys

conn = None
cur = None
imdb_tables = []
imdb_ddl = []
imdb_queries = []
datapathprefix = "../../third_party/imdb/"

def createschema():
    try:
        fname = datapathprefix  +  "imdb_tables_ddl"
        f = open(fname,"r")
        imdb_ddl = f.read().splitlines()
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    for d in imdb_ddl:
        print(d)
        cur.execute(d)

def loaddata():
    try:
        fname = datapathprefix  +  "imdb_table_names"
        f = open(fname,"r")
        imdb_tables = json.loads(f.read())
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    
    print('loading data')
    for t in imdb_tables:
        print(t)
        cur.execute("COPY " + t + " FROM '" + data_file_name + "' DELIMITER ',' ESCAPE '\\';");

def readqueries():
    global imdb_queries
    try:
        fname = datapathprefix  +  "imdb_queries"
        f = open(fname,"r")
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    print(f"Loading the queries from '{fname}'")
    imdb_queries = f.read().splitlines()
    print(f"Loaded {len(imdb_queries)} queries")

def runqueries():
    global imdb_queries
    print('Start running the queries')
    for q in imdb_queries:
        clk = time.time()
        qry= q[15:-1]
        qry = qry.replace('\\n',' ')
        print(q[:15])
        print(qry)
        cur.execute(qry)
        res = cur.fetchall()
        for r in res:
            print(r)
        print(f"QRY {q[4:5]} {time.time() - clk} ms")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Identify the full path to the data definitions")
        exit(-1)

    conn = monetdbe.connect(':memory:', autocommit=True)
    if not conn:
        print('Could not access the database')
        exit -1
    cur = conn.cursor()
    # WRONG   conn.execute("""BEGIN TRANSACTION;""")
    cur.transaction()
    createschema()
    # loaddata() TODO
    readqueries()
    runqueries()
    cur.commit()
    # WRONG conn.execute("""COMMIT;""")
    conn.close()
