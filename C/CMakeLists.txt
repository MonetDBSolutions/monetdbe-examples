#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2020 MonetDB Solutions B.V.
#]]

add_executable(getstarted getstarted.c)
target_link_libraries(getstarted
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_getstarted getstarted)

add_executable(kickoff kickoff.c)
target_link_libraries(kickoff
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_kickoff kickoff)

add_executable(helloworld helloworld.c)
target_link_libraries(helloworld
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_helloworld helloworld)

add_executable(tumble tumble.c)
target_link_libraries(tumble
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_tumble tumble)

add_executable(logs logs.c)
target_link_libraries(logs
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
#needs a benchmark file
#add_test(run_logs logs)

add_executable(copy_into copy_into.c)
target_link_libraries(copy_into
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_copy_into copy_into)

add_executable(concurrent concurrent.c)
target_link_libraries(concurrent
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe
    -lpthread)
add_test(run_copy_into_threading concurrent)

add_subdirectory(tpch)
add_subdirectory(imdb)
