# mdbe-examples
A small collection of C programs to illustrate MonetDBe

In June 2020 we annouced the development of the embedded version of MonetDB. 
The functionality offered encompases the complete server functionality, but using
a directly loadable library libmonetdbe.so

To illustrate the functionality, and give a bootstrep for users to use it in their
daily life, we collected a handful of examples to try it out.

| program | description|
| ------------- | ----------------------------------------------------------- |
| helloword.c  |just start an :inmemory: database and execute a minimal query|
|  helloall.c    |illustrates the same, but switching between different databases|
|  voc.c |a more extensive example to load the MonetDB VOC turtorial|
|  mylist.c    |illustrates how to create table in :memory: backup and re-use|
|  duckdb.c  |a comparison with another embedded database|

Feel free to propose other tiny examples that help users to explore the system.
