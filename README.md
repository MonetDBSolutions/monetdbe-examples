# monetdbe-examples
A small collection of C programs to illustrate MonetDBe

In June 2020 we annouced the development of the embedded version of MonetDB. 
The functionality offered encompases the complete server functionality, but using
a directly loadable library libmonetdbe.so

To illustrate the functionality, and give a bootstrep for users to use it in their
daily life, we collected a handful of examples to try it out.

| program | description|
| ------------- | ----------------------------------------------------------- |
| helloword.c  |just start/stop an :inmemory: database |
|  helloall.c    |illustrates the same, but switching between different databases|
|  kickoff.c    |the kickoff example to test part of the api|
|  duckdb.c  |a comparison with another embedded database|
|  voc.c |a more extensive example to load the MonetDB VOC tutorial|

Feel free to propose other tiny examples that help users to explore the system.

## Installation
You should have obtained a copy of libmonetdbe.so from the MonetDB download area
or created one using a build from source. The location of this library should be made 
known in src/CMakeList.c
