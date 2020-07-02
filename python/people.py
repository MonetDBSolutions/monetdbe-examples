"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.


 For an explanation of the command arguments see the documentation

 This example is based on https://team-mayes.github.io/che_696/html/notebooks/lecture17_databases.html
 It illustrates some basic database interactions albeit with re-establishing a connection with database repeatedly.
"""

import monetdbe
import os

database = 'test.mdbe'

# Removes the database if it already exists
if os.path.exists(database):
    os.system(f'rm -rf {database}')


with monetdbe.connect(database) as conn:
    conn.set_autocommit(True)
    cursor = conn.cursor()
    cursor.execute(
        """CREATE TABLE People
        (first_name TEXT, last_name TEXT, job TEXT, age NUMERIC)""")
    cursor.execute(
        """INSERT INTO People VALUES (?,?,?,?)""",
        ('Bradley', 'Dice', 'Guest Lecturer', 25))

with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT * FROM People""")
    print(results.fetchall())

# Let's add lots of people
famous_actors = [
    ('Robert', 'Downey Jr.', 'Iron Man', 53),
    ('Chris', 'Evans', 'Captain America', 37),
    ('Scarlett', 'Johansson', 'Black Widow', 33),
    ('Samuel', 'Jackson', 'Nick Fury', 69),
    ('Benedict', 'Cumberbatch', 'Dr. Strange', 42),
    ('Brie', 'Larson', 'Captain Marvel', 29),
    ('Chadwick', 'Boseman', 'Black Panther', 40),
]
# Yes, I'm a Marvel fan
with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    cursor.executemany("""INSERT INTO People VALUES (?,?,?,?)""", famous_actors)

with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    cursor.execute(
        """SELECT * FROM People""")
    print(cursor.fetchall())


with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    cursor.execute(
        """UPDATE People SET first_name = ? WHERE first_name = ? AND last_name = ?""",
        ("Samuel L.", "Samuel", "Jackson"))

age = 45 # input('People older than:')
with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    results = cursor.execute(
        """SELECT first_name, age FROM People WHERE age >= ?""",
        (age,))
    for r in results:
        print(r)

with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT first_name, last_name FROM People WHERE last_name = ?""", ("Jackson",))
    print(list(results))


with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT COUNT(*) FROM People""")
    print('Total count of people:', results.fetchall())
    results = cursor.execute(
        """SELECT SUBSTR(first_name, 1, 1) AS first_letter, COUNT(*) FROM People GROUP BY first_letter""")
    print('Count by first letters of first names:', results.fetchall())
    results = cursor.execute("""SELECT AVG(age) FROM People""")
    print('Average age of people:', results.fetchall()[0][0])
    results = cursor.execute("""SELECT SUM(age) FROM People""")
    print('Summed ages of people:', results.fetchall())

# Show us the guts of the database! This command is SQLite-specific.
with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    results = cursor.execute("""SELECT count(*) FROM tables""")
    print(list(results))

with monetdbe.connect(database) as conn:
    cursor = conn.cursor()
    cursor.execute("""DELETE FROM People WHERE first_name = ?""", ("Bradley",))
    results = cursor.execute("""SELECT COUNT(*) FROM People""")
    print('Total count of people after removing Bradley:', results.fetchall())
    # You can't rename or remove columns in sqlite, but this is how you would do it in most SQL databases:
    #cursor.execute("""ALTER TABLE people DROP COLUMN age""")
    cursor.execute("""DROP TABLE People""")
    print('The table "people" has been dropped.')


