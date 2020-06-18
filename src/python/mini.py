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

    # alternative interface
    # for i in range(c.nrofrows):
    #   for col in c.fetchcolumns():
    #       print(col[r])
    
    c.close()
    conn.close()
