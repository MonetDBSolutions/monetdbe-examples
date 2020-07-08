"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This trivial program can be used to check if all the basic ingredients
 for using MonetDB/e has been available and accessible.

 For an explanation of the command arguments see the documentation
 https://monetdbe.readthedocs.io/en/latest/introduction.html
"""
import monetdbe

if __name__ == "__main__":
    conn1 = monetdbe.connect('db1', autocommit=True)
    if not conn1:
        print('Could not access database db1')
        exit -1
    print("Connection to db1 established")
    cur1 = conn1.cursor()
    cur1.execute("create table tmp1(i integer)")
    cur1.execute("insert into tmp1 values(1),(2),(30)")
    cur1.execute("select * from tmp1")
    row = cur1.fetchall()
    for r in row:  
        print(r)

    # open second connection to same database
    conn2 = monetdbe.connect('db1', autocommit=True)
    if not conn2:
        print('Could not access database db1')
        exit -1
    print("Second connection to db1 established")
    cur2 = conn2.cursor()
    cur2.execute("select * from tmp1")
    row = cur2.fetchall()
    for r in row:  
        print(r)
    
    # and the first connection can still be used
    cur1.execute('select sum(i) from tmp1')
    res1 = cur1.fetchall()
    print(f"sum {res1}")

    # open a connection to another database
    conn3 = monetdbe.connect('db2')
    if not conn3:
        print('Could not access database "db2" ')
        exit -1

    print("Connection to db2 established")
    cur3 = conn3.cursor()
    cur3.execute("create table tmp3(i string)")
    cur3.execute("insert into tmp3 values('hello'),('world')")
    cur3.execute("select * from tmp3")
    row = cur3.fetchall()
    for r in row:  
        print(r)

    # and the first connection can still be used
    try:
        cur1.execute('select * from tmp1')
        res3 = cur1.fetchall()
        print(f"sum {res3}")
        cur1.execute('drop table tmp1');
        cur2.execute('drop table tmp1');
    except monetdbe.DatabaseError as msg:
        print(f"Error, we should keep the connection to db1 around")

    conn1.close()
    conn2.close()
    conn3.close()
