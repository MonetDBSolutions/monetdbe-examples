cmake_minimum_required(VERSION 2.8.7)

project(dbgen C)
cmake_policy(SET CMP0063 NEW)

include_directories(include)

add_definitions(-D_FILE_OFFSET_BITS=64
                -DDBNAME=dss
                -DLINUX
                -DMONETDB
                -DTPCH)

set(CMAKE_BUILD_TYPE "Release")
add_library(dbgen STATIC
            build.c
            dbgen.c
            bm_utils.c
            rnd.c
            speed_seed.c
            text.c
            permute.c
            rng64.c)

target_link_libraries(dbgen
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::monetdbe)
set_property(TARGET dbgen PROPERTY C_STANDARD 99)
