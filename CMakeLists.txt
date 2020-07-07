#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2020- MonetDB Solutions B.V.
#]]

cmake_minimum_required(VERSION 3.12)

project(MonetDB-extend VERSION "${MONETDB_VERSION}" LANGUAGES C)
set(C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 99)

find_package(MonetDB CONFIG REQUIRED)
#add_subdirectory(python)
#add_subdirectory(notebooks)
include_directories(third_party/dbgen/include)
include_directories(third_party/imdb/include)
add_subdirectory(third_party)
add_subdirectory(C)
