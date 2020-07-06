set(CMAKE_BUILD_TYPE "DEBUG")
add_executable(sf1 sf1.c)
target_link_libraries(sf1
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe
    dbgen)
add_test(run_sf1 sf1)
