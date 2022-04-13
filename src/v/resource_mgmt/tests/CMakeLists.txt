rp_test(
  UNIT_TEST
  BINARY_NAME test_resource_mgmt
  SOURCES rate_algorithm_test.cc
  DEFINITIONS BOOST_TEST_DYN_LINK
  LIBRARIES v::seastar_testing_main Boost::unit_test_framework
  LABELS resource_mgmt
  )
