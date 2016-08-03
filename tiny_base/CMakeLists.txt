cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

project(tiny_base VERSION 1.0 LANGUAGES CXX C)

add_executable(tiny_base
               main.cc
               internal/table_manager.cc
               internal/page_manager.cc
               sql/database_engine.cc
               utils/file_util.cc)

target_include_directories(tiny_base PRIVATE internal sql utils)

if(CMAKE_COMPILER_IS_GNUCXX)
  target_compile_options(tiny_base PRIVATE -std=c++11 -std=c++1y)
  target_link_libraries(tiny_base PRIVATE stdc++fs)
endif()
