include(rpcgen)
rpcgen(
  TARGET debug_gen
  IN_FILE ${CMAKE_CURRENT_SOURCE_DIR}/self_test_gen.json
  OUT_FILE ${CMAKE_CURRENT_BINARY_DIR}/self_test_gen_service.h
  INCLUDES ${CMAKE_BINARY_DIR}/src/v
  )

v_cc_library(
  NAME debug
  SRCS
    logger.cc
  DEPS
    v::rpc
    debug_gen
  )
