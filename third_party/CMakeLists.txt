# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

if(NOT AMALGAMATION_BUILD)
endif()

if(NOT WIN32 AND NOT SUN)
  #add_subdirectory(imdb)
  add_subdirectory(dbgen)
endif()
