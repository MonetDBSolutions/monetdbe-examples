# monetdbe-examples

The MonetDB server code has been split into a separately useable single library
for integration with a host language.
The functionality offered encompasses the complete server functionality, but using
a directly loadable library libmonetdbe.so or 'pip install monetdbe'

To test and illustrate the functionality, and give a bootstrap for users to use it in their
daily life, we collected a handful of small examples to try it out.

| program | description|
| ------------- | ----------------------------------------------------------- |
| helloworld.{c, py}  |just start/stop an :inmemory: database |
| getstarted.{c, py}  |a simple application framework |
| people.py |an educational example based on a people  database |
| movies.py |an edcucational example based on a movie  database |
| imdb.py |a snippet of the infamous movie database |
| tumble.c    |switching between different databases|
| kickoff.c    |the kickoff example to test part of the api|
| tpch-sf1.py |the prototypical TPCH scale-factor 1 data warehouse|
| taxi.py |the NYC taxi example  |
| mgbench.py | A machine generated database benchmark |
| logs.c | a snippet of the inhouse qa database |
| cache.{c, py}  |a simple caching database application |
| myserver.c | a minimalistic database server setup |
| multiconnections.c | multiple connections from same program |

Feel free to propose other tiny examples that help users to explore the system.
As a more extensive QA activity is ongoing, it may lead to smaller changes in the
interfaces. Please report any issue with the code on stack-overflow.

## Installation
For details on installing monetdbe see https://monetdbe.readthedocs.io/en/latest/installation.html.
A quick guide to compile and install from source to create a library for your C application:
```
git clone https://github.com/MonetDB/MonetDB MonetDB
cd MonetDB
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
```

This results in the libmonetdbe.so library to be created and stored  .../lib64/libmonetdbe.so
or .../build/tools/monetdbe/libmonetdbe.so. The necessary include file monetdbe.h is located
in the same place.

Continue with the compilation of the examples using
```
mkdir build && cd build
cmake -DCMAKE_PREFIX_PATH=$prefix .. #were $prefix is your monetdb install path
cmake --build .
```
