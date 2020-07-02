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
import argparse
import monetdbe
import json
import time 
import sys
import os


debug = False
sql = False
conn = None
cur = None
imdb_tables = []
imdb_ddl = []
imdb_queries = []
dataprefix = "unkown"

parser = argparse.ArgumentParser(
    description='IMDB sample ',
    epilog='''

    This program loads and executes the IMDB database sample.
    The default calling sequences is:
    python imdb.py --load --run --database=:memory:
    ''')

parser.add_argument('--debug', help='Trace processing ', action='store_true')
parser.add_argument('--sql', help='Produce SQL only ', action='store_true')
parser.add_argument('--load', help='Create schema and load data', action='store_true')
parser.add_argument('--run', help='Run the queries, assume not in :memory: ', action='store_true')
parser.add_argument('--database', type=str, help='Only use :memory: schema or local directory', default=':memory:')
parser.add_argument('--prefix', type=str, help='Absolute path to the project directory', default=None)


def createschema():
    if debug:
        print('create schema')
    fname = dataprefix  +  "third_party/imdb/imdb_tables_ddl"
    try:
        f = open(fname,"r")
        imdb_ddl = f.read().splitlines()
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    for d in imdb_ddl:
        if debug or sql:
            print(d)
        if not sql:
            cur.execute(d)

def loaddata():
    try:
        fname = dataprefix  +  "third_party/imdb/imdb_table_names"
        f = open(fname,"r")
        imdb_tables = json.loads(f.read())
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    
    if debug:
        print('loading data')
    for t in imdb_tables:
        data_file_name = dataprefix + "data/imdb/" + t + ".csv.gz"
        clk = time.time()
        if debug or sql:
            print("COPY INTO " + t + " FROM '" + data_file_name + "' DELIMITERS ',','\\n','\"' NULL AS '' BEST EFFORT;")
        if sql:
            continue
        cur.execute("COPY INTO " + t + " FROM '" + data_file_name + "' DELIMITERS ',','\\n','\"' NULL AS '' BEST EFFORT;")
        cur.execute("SELECT COUNT(*) FROM " + t)
        cnt = cur.fetchall()[0][0]
        cur.execute('SELECT count(*) from sys.rejects')
        rej = cur.fetchall()[0][0]
        if rej:
            cur.execute('SELECT * from sys.rejects')
            rtup = cur.fetchall()
            for r in rtup:
                print(r)
        if debug:
            print(f"LOADED {data_file_name} {'%6.3f' % (time.time() - clk)} sec {cnt} tuples {rej} rejects")
        else:
            print(f"{t},{'%6.3f' % (time.time() - clk)},{cnt},{rej}")
            

def readqueries():
    global imdb_queries
    if debug:
        print('read queries')
    try:
        fname = dataprefix  +  "third_party/imdb/imdb_queries"
        f = open(fname,"r")
    except IOError as msg:
        print(f"Could not open/read {fname}")
        exit(-1)
    imdb_queries = f.read().splitlines()
    if debug:
        print(f"Loaded {len(imdb_queries)} queries")

def runqueries():
    global imdb_queries
    if debug:
        print('run queries')
    for q in imdb_queries:
        clk = time.time()
        qry= q[15:-1]
        if sql:
            print(f"--{q[3:10]}")
            qry = qry.replace('\\n','\n')
            print(qry)
            continue
        qry = qry.replace('\\n',' ')
        if debug:
            print(q[:15])
            print(qry)
        cur.execute(qry)
        if debug:
            res = cur.fetchall()
            for r in res:
                print(r)
        if debug:
            print(f"QRY {q[3:10]} {'%6.3f' % (time.time() - clk)} sec")
        else:
            print(f"{q[3:10]},{'%6.3f' % (time.time() - clk)}")


if __name__ == "__main__":
    args = parser.parse_args()
   
    debug = args.debug
    sql = args.sql
    if not args.prefix:
        dataprefix =  os.getcwd()[:-6]
        print(dataprefix)
    else:
        dataprefix = args.prefix

    conn = monetdbe.connect(args.database, autocommit=True)
    if not conn:
        print('Could not access the database')
        exit -1
    cur = conn.cursor()
    cur.transaction()
    if args.load:
        createschema()
        loaddata() 
    if args.run:
        readqueries()
        runqueries()
    cur.commit()
    conn.close()
