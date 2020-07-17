"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 Inspired by a short report on the MonetDB list
"""
import monetdbe
import time
import random
import os

if __name__ == "__main__":
    
        # store the result in an :memory: structure
        conn = monetdbe.connect(':memory:')
        cur = conn.cursor()
        clk =  time.time()
        cur.execute("create table tmp1(v1 string, v2 string, v3 string, v4 string);")

        #f= open('script', 'w')
        pool = [str(c) for c in  '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ']
        for i in range(10000):
            s1 = pool * int(3200/len(pool))
            random.shuffle(s1)
            s1 = ''.join(s1)
            s2 = pool * int(3200/len(pool))
            random.shuffle(s2)
            s2 = ''.join(s2)
            s3 = pool * int(3200/len(pool))
            random.shuffle(s3)
            s3 = ''.join(s3)
            s4 = pool * int(3200/len(pool))
            random.shuffle(s4)
            s4 = ''.join(s4)
        print("Clk %5.3f" % (time.time() - clk))

        for i in range(10000):
            s1 = pool * int(3200/len(pool))
            random.shuffle(s1)
            s1 = ''.join(s1)
            s2 = pool * int(3200/len(pool))
            random.shuffle(s2)
            s2 = ''.join(s2)
            s3 = pool * int(3200/len(pool))
            random.shuffle(s3)
            s3 = ''.join(s3)
            s4 = pool * int(3200/len(pool))
            random.shuffle(s4)
            s4 = ''.join(s4)
            #f.write(f"insert into tmp1 values('{s1}','{s2}','{s3}','{s4}');\n")
            cur.execute(f"insert into tmp1 values('{s1}','{s2}','{s3}','{s4}')")
        print("Clk %5.3f" % (time.time() - clk))
        cur.execute("select count(*) from tmp1")
        print(cur.fetchall())


