"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

This program is intended to load the imdb database sample 
The DDL contains reserved words that need to be quoted.
Awaiting the auto-quoting option in monetdbe-python

 For an explanation of the command arguments see MonetDBe/Python documentation
"""
import monetdbe
import json
import time 

conn = None
imdb_tables = []
imdb_ddl = []
imdb_queries = []
datapathprefix = "../../third_party/imdb"

def createschema():
    f = open(datapathprefix  +  "imdb_tables_ddl","r")
    imdb_ddl = f.read().splitlines()
    for d in imdb_ddl:
        conn.execute(d)

def loaddata():
    f = open(datapathprefix  +  "imdb_table_names","r")
    imdb_tables = json.loads(f.read())
    for t in imdb_tables:
        conn.Query("COPY " + table_name + " FROM '" + data_file_name + "' DELIMITER ',' ESCAPE '\\';");

def readqueries():
    f = open(datapathprefix  +  "imdbs_queries","r")
    imdb_queries = json.loads(f.read())

def runqueries():
    for q in imdb_queries:
        clk = time.time()
        print(q)
        print(f"QRY {q[4:5]} {time.time() - clk} ms")


if __name__ == "__main__":
    conn = monetdbe.connect(':memory:')
    if not conn:
        print('Could not access the memory')
        exit -1
    conn.execute("""BEGIN TRANSACTION;""")
    createschema()
    loaddata()
    readqueries()
    runqueries()
    conn.execute("""COMMIT;""")
    conn.close()
