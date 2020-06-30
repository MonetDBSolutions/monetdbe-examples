"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 An embedded application can act a cache with a remote server.
 For this we need two database objects, one :memory: and an URL link to the remote.

 For an explanation of the command arguments see MonetDBe/Python documentation
"""
import monetdbe

if __name__ == "__main__":
    print('This experiment is on hold until we support remote connections')
    exit(-1)
    
    try:
        remote = monetdbe.connect("monetdb://localhost:5000/sf1?user=monetdb&password=monetdb")
        curr = remote.cursor()
        schema = curr.execute("call sys.describe(\"sys\",\"lineitem\"")
        data = curr.execute("select line_no from sf1 where line_no < 10")
        
        if remote.error or schema.error or data.error:
            print("Could not access the remote database")
            exit (-1)

        schemadef = schema.fetchone()

        # store the result in an :memory: structure
        local = monetdbe.connect(None)
        curl = local.cursor()
        schema = curl.execute(schemadef)
        curl.append("sys","lineitem", data)

        if local.error or schema.error or result.error:
            print("Construction of the local cache failed\n")
            exit (-1)

        print("Obtained tuples from the remote")

    except monetdbe.exceptions.OperationalError as msg:
        print(f"Error encountered {msg}")
