```
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 This trivial program can be used to check if all the basic ingredients
 for using MonetDBe has been available and accessible.

 For an explanation of the command arguments see MonetDBe/Python documentation
```
import monetdbe


if __name__ == "__main__":
	conn = monetdbe.open(':memory:', None)
	if not conn or conn.error:
		print('Could not access the memory')
		exit -1

	print("hello world, we have a lift off\n MonetDBe has been started\n")

	try:
		monetdbe.close(conn)
	except MonetDBe.databaseError msg:
		print(msg)
		exit(-1)
	print("hello world, we savely returned\n")
