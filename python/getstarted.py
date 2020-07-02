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
    conn = monetdbe.connect(':memory:')
    if not conn:
        print('Could not access the memory')

    c = conn.cursor()
    c.execute("CREATE TABLE integers(i INTEGER, j INTEGER);")
    c.execute("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL);")
    c.execute("SELECT * FROM integers")

    # retrieve the description
    for r in c.description:
        print(r)

    # retrieve the data by rows
    for r in c.fetchall():
        print(r)

    # retrieve the rows explicitly
    # TODO for r in c.fetchrows():
    #   print(r)

    # TODO alternative interface
    # for i in range(c.nrofrows):
    #   for col in c.fetchcolumns():
    #       print(col[r])
    
    c.close()
    conn.close()
