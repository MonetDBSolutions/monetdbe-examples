# monetdbe-examples
A small collection of C programs to illustrate MonetDBe

In June 2020 we announced an alpha-release of the embedded version of MonetDB. 
The functionality offered encompases the complete server functionality, but using
a directly loadable library libmonetdbe.so or 'pip install monetdbe'

To illustrate the functionality, and give a bootstrep for users to use it in their
daily life, we collected a handful of small examples to try it out.
A more extensive QA activity is ongoing, which may lead to smaller changes in the
interfaces. Please report any issue with the code on stack-overflow.

The repository comes with two flavors or mirrored implementation

| program | description|
| ------------- | ----------------------------------------------------------- |
| helloworld.{c.py}  |just start/stop an :inmemory: database |
| tumble.c    |illustrates the same, but switching between different databases|
| kickoff.{c.py}    |the kickoff example to test part of the api|
| mini.{c.py}  |a simple application framework |
| cache.{c.py}  |a simple caching database application |
| people.py |an example based on a people  database |
| movies.py |an example based on a movie  database |
| voc.{c.py} |a more extensive example based on the MonetDB VOC tutorial|
| tpch-sf1.py |the prototypical TPCH scale-factor 1 data warehouse|

Feel free to propose other tiny examples that help users to explore the system.

## Installation
The Python integration is simplified with the binary wheel, just pip install monetdbe.

For the time being, you need access to the monetdb repository to build the libmonetdbe.so.
The location of this library should be made known in src/CMakeList.c

The one-liner to create MonetDB after downloading the sources:

mkdir build && cd build
CMAKE_PREFIX_PATH=/tmp/monetdb/include/cmake/ cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build .
