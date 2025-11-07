cmake_minimum_required(VERSION 3.15)
project(KV_Server CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_path(MYSQL_INCLUDE_DIR mysql_connection.h
    HINTS /usr/include/cppconn /usr/local/include/cppconn
    REQUIRED)
find_library(MYSQL_LIBRARY NAMES mysqlcppconn
    HINTS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib
    REQUIRED)
find_package(Threads REQUIRED)

include(FetchContent)
FetchContent_Declare(
  httplib
  GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
  GIT_TAG        v0.15.3
)
FetchContent_MakeAvailable(httplib)

add_executable(server server.cpp)
target_link_libraries(server 
    PRIVATE 
    httplib 
    ${MYSQL_LIBRARY}
    Threads::Threads
)
target_include_directories(server 
    PRIVATE 
    ${MYSQL_INCLUDE_DIR}
)

add_executable(client client.cpp)
target_link_libraries(client PRIVATE httplib)