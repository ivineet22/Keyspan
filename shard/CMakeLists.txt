cmake_minimum_required(VERSION 3.20)
project(shard LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(shard
    src/main.cpp
    src/log.cpp
    src/engine.cpp
    src/rpc.cpp
)

target_include_directories(shard PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    "C:/Program Files/PostgreSQL/17/include"
)

target_link_libraries(shard PRIVATE
    "C:/Program Files/PostgreSQL/17/lib/libpq.lib"
    ws2_32
    secur32
    crypt32
    wldap32
    delayimp
)

if(MSVC)
    target_link_options(shard PRIVATE "/DELAYLOAD:libpq.dll")
    set_target_properties(shard PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_CURRENT_SOURCE_DIR}/../bin
        RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_CURRENT_SOURCE_DIR}/../bin
    )
endif()
