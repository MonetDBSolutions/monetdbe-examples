cmake_minimum_required(VERSION 2.8.7)

project(imdb C)
cmake_policy(SET CMP0063 NEW)
include_directories(include)

set(CMAKE_BUILD_TYPE "DEBUG")
add_library(imdblib STATIC imdb.c)

target_link_libraries(imdblib
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
set_property(TARGET imdblib PROPERTY C_STANDARD 99)
