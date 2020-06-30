"""
 This Source Code Form is subject to the terms of the Mozilla Public
 License, v. 2.0.  If a copy of the MPL was not distributed with this
 file, You can obtain one at http://mozilla.org/MPL/2.0/.

 (c) 2020- MonetDB Solutions B.V.

 An embedded application can act a cache with a remote server.
 For this we need two database objects, one :memory: and an URL link to the remote.

 For an explanation of the command arguments see MonetDBe/Python documentation

 Adapted from https://team-mayes.github.io/che_696/html/notebooks/lecture17_databases.html

 For an explanation of the command arguments see the documentation
 https://monetdbe.readthedocs.io/en/latest/introduction.html
"""

import monetdbe
import os

database = '/tmp/movies.mdbe'

# Removes the database if it already exists
if os.path.exists(database):
    os.system(f'rm -rf {database}')

with monetdbe.connect(database) as conn:
    # Here we create a primary key, and use "NOT NULL" to prevent inserting invalid data
    conn.set_autocommit(True)
    conn.execute(
        """CREATE TABLE Movies
        (id SERIAL, title TEXT NOT NULL, "year" INTEGER NOT NULL)""")
    conn.executemany(
        """INSERT INTO Movies (title, "year") VALUES (?,?)""",
        [('Iron Man', 2008),
         ('The Incredible Hulk', 2008),
         ('Iron Man 2', 2010),
         ('Thor', 2011),
         ('Captain America: The First Avenger', 2011),
         ('The Avengers', 2012),
         ('Iron Man 3', 2013),
         ('Captain America: The Winter Soldier', 2014),
         ('Avengers: Age of Ultron', 2015),
         ('Captain America: Civil War', 2016),
         ('Doctor Strange', 2016),
         ('Black Panther', 2018),
         ('Avengers: Infinity War', 2018),
        ])


with monetdbe.connect(database) as conn:
    results = conn.execute("""SELECT * FROM Movies""")
    print(results.fetchall())

with monetdbe.connect(database) as conn:
    results = conn.execute("""SELECT * FROM Movies ORDER BY "year" DESC""")
    print(results.fetchall())

with monetdbe.connect(database) as conn:
    conn.execute(
        """CREATE TABLE Actors
        (id SERIAL,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        "character" TEXT NOT NULL,
        age REAL NOT NULL)""")
    conn.executemany(
        """INSERT INTO Actors (first_name, last_name, "character", age) VALUES (?,?,?,?)""",
        [('Robert', 'Downey Jr.', 'Iron Man', 53),
         ('Chris', 'Evans', 'Captain America', 37),
         ('Scarlett', 'Johansson', 'Black Widow', 33),
         ('Samuel L.', 'Jackson', 'Nick Fury', 69),
         ('Benedict', 'Cumberbatch', 'Dr. Strange', 42),
         ('Brie', 'Larson', 'Captain Marvel', 29),
         ('Chadwick', 'Boseman', 'Black Panther', 40)
        ])
    # ...and print the results
    results = conn.execute("""SELECT * FROM Actors""")
    print(results.fetchall())

    conn.execute(
        """CREATE TABLE MovieActors
        (id SERIAL, movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL)""")
    conn.executemany(
        """INSERT INTO MovieActors (movie_id, actor_id) VALUES (?,?)""",
        [(1, 1), (2, 1), (3, 1), (6, 1), (7, 1), (9, 1), (10, 1), (13, 1), (5, 2), (6, 2), (8, 2), (9, 2), (10, 2), (13, 2),
        (3, 3), (6, 3), (8, 3), (9, 3), (10, 3), (13, 3), (1, 4), (3, 4), (4, 4), (5, 4), (6, 4), (8, 4), (9, 4), (13, 4),
        (11, 5), (13, 5), (10, 7), (12, 7), (13, 7)])
    # ...and print the results
    results = conn.execute("""SELECT * FROM MovieActors""")
    print(results.fetchall())

    results = conn.execute(
        """SELECT * FROM MovieActors
        JOIN Movies ON MovieActors.movie_id = Movies.id
        JOIN Actors ON MovieActors.actor_id = Actors.id""")
    print(results.fetchall())
