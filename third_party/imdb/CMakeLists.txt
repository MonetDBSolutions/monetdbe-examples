cmake_minimum_required(VERSION 2.8.7)

project(imdb C)
cmake_policy(SET CMP0063 NEW)
include_directories(include)

set(CMAKE_BUILD_TYPE "DEBUG")
add_library(imdb STATIC imdb.c)

target_link_libraries(imdb
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
set_property(TARGET imdb PROPERTY C_STANDARD 99)
