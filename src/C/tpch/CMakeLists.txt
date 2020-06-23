include_directories(../../../third_paty/dbgen/include)

add_executable(sf1 sf1.c)
target_link_libraries(sf1
  PRIVATE
    dbgen
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
add_test(run_sf1 sf1)
