#[[
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 2020 MonetDB Solutions B.V.
#]]

add_executable(kickoff kickoff.c)
target_link_libraries(kickoff
  PRIVATE
    MonetDB::monetdb_config_header
    MonetDB::embedded)
add_test(run_kickoff kickoff)
