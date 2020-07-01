# monetdbe-examples
A small collection of C and Python programs to illustrate MonetDB/e.

The MonetDB server code has been split into a separately useable single library
for integration with a host language.
The functionality offered encompasses the complete server functionality, but using
a directly loadable library libmonetdbe.so or 'pip install monetdbe'

To illustrate the functionality, and give a bootstrap for users to use it in their
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
The Python integration is simplified with the binary wheel.
Just [pip install monetdbe](https://pypi.org/project/monetdbe/).

For the time being, you need access to the monetdb repository to build the libmonetdbe.so
for embedding it in your C program.

The snippet to create MonetDB after downloading the sources and from its root directory:

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
The location of this library should be made known in src/CMakeList.c, e.g.

```
set(monetdbe_public_headers
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/monetdbe.h>
  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/monetdb/monetdbe.h>)
```
