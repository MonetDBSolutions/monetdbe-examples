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
import os

conn = None
cur = None
imdb_tables = []
imdb_ddl = []
imdb_queries = []
dataprefix = "unkown"

def createschema():
    fname = dataprefix  +  "third_party/imdb/imdb_tables_ddl"
    try:
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
        fname = dataprefix  +  "third_party/imdb/imdb_table_names"
        f = open(fname,"r")
        imdb_tables = json.loads(f.read())
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    
    print('loading data')
    for t in imdb_tables:
        data_file_name = dataprefix + "data/imdb/" + t + ".csv.gz"
        clk = time.time()
        print("COPY INTO " + t + " FROM '" + data_file_name + "' DELIMITERS ',' NULL AS  '\\\\N';")
        cur.execute("COPY INTO " + t + " FROM '" + data_file_name + "' DELIMITERS ',' NULL AS '\\\\N';")
        print(f"LOADED {data_file_name} {time.time() - clk} ms")

def readqueries():
    global imdb_queries
    try:
        fname = dataprefix  +  "third_party/imdb/imdb_queries"
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
    dataprefix =  os.getcwd()[:-10]
    print(dataprefix)

    conn = monetdbe.connect(':memory:', autocommit=True)
    if not conn:
        print('Could not access the database')
        exit -1
    cur = conn.cursor()
    # WRONG   conn.execute("""BEGIN TRANSACTION;""")
    cur.transaction()
    createschema()
    loaddata() 
    readqueries()
    # runqueries()
    cur.commit()
    # WRONG conn.execute("""COMMIT;""")
    conn.close()
