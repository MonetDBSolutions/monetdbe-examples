set(CMAKE_BUILD_TYPE "DEBUG")
add_executable(imdb imdb.c)
target_link_libraries(imdb
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe
    imdblib)
#needs a imdb data file
#add_test(run_imdb imdb)
