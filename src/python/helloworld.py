"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This trivial program can be used to check if all the basic ingredients
 for using MonetDB/e has been available and accessible.

 For an explanation of the command arguments see the Python documentation fo MonetDB/e
"""
import monetdbe

if __name__ == "__main__":
    conn = monetdbe.connect(':memory:')
    if not conn:
        print('Could not access the memory')
        exit -1

    print("hello world, we have a lift off\nMonetDB/e has been started\n")

    try:
        conn.close()
    except DatabaseError as msg:
        print(msg)
        exit(-1)
    print("hello world, we savely returned\n")
