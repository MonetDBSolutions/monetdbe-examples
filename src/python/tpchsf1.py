"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This program is the canonical data warehouse benchmark TPCH SF1.
 To prepare, use the dbgen program to create the necessary data files in ../../third-party/tpch
 For an explanation of the command arguments see MonetDBe/Python documentation
""" 

import monetdbe

data="../../thidparty/dbgen"
if __name__ == "__main__":
    conn = monetdbe.connect(':memory:')
    if not conn:
        print('Could not access the memory')

    
    c.close()
    conn.close()
