cmake_minimum_required(VERSION 3.0)

project(RBTreeRedisModule)

set(MODULE_SRCS rbtree_redis.cpp)
set(CMAKE_INSTALL_PREFIX /var/redis)

add_library(rbtree MODULE ${MODULE_SRCS})
set_target_properties(rbtree PROPERTIES PREFIX "")
target_link_options(rbtree PRIVATE "LINKER:-shared,-Bsymbolic")

install(TARGETS rbtree DESTINATION lib)
