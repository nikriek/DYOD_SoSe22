#include <limits>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/value_segment.hpp"

namespace opossum {

class StorageValueSegmentTest : public BaseTest {
 protected:
  ValueSegment<int32_t> int_value_segment;
  ValueSegment<std::string> string_value_segment;
  ValueSegment<double> double_value_segment;
};

TEST_F(StorageValueSegmentTest, GetSize) {
  EXPECT_EQ(int_value_segment.size(), 0u);
  EXPECT_EQ(string_value_segment.size(), 0u);
  EXPECT_EQ(double_value_segment.size(), 0u);
}

TEST_F(StorageValueSegmentTest, AddValueOfSameType) {
  int_value_segment.append(3);
  EXPECT_EQ(int_value_segment.size(), 1u);

  string_value_segment.append("Hello");
  EXPECT_EQ(string_value_segment.size(), 1u);

  double_value_segment.append(3.14);
  EXPECT_EQ(double_value_segment.size(), 1u);
}

TEST_F(StorageValueSegmentTest, AddValueOfDifferentType) {
  int_value_segment.append(3.14);
  EXPECT_EQ(int_value_segment.size(), 1u);
  EXPECT_THROW(int_value_segment.append("Hi"), std::exception);

  string_value_segment.append(3);
  string_value_segment.append(4.44);
  EXPECT_EQ(string_value_segment.size(), 2u);

  double_value_segment.append(4);
  EXPECT_EQ(double_value_segment.size(), 1u);
  EXPECT_THROW(double_value_segment.append("Hi"), std::exception);
}

TEST_F(StorageValueSegmentTest, MemoryUsage) {
  int_value_segment.append(1);
  EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{4});
  int_value_segment.append(2);
  EXPECT_EQ(int_value_segment.estimate_memory_usage(), size_t{8});
}

TEST_F(StorageValueSegmentTest, SubscriptOperator) {
  int_value_segment.append(1337);
  int_value_segment.append(1338);
  EXPECT_EQ(int_value_segment[0], AllTypeVariant(1337));
  EXPECT_EQ(int_value_segment[1], AllTypeVariant(1338));
}

TEST_F(StorageValueSegmentTest, Values) {
  int_value_segment.append(1337);
  int_value_segment.append(1338);

  auto values = int_value_segment.values();
  auto expected_variants = std::vector<int>{1337, 1338};

  ASSERT_EQ(values.size(), expected_variants.size())
      << "Vectors int_value_segment.values() and expected_variants are of unequal length";
  for (size_t index = 0; index < values.size(); ++index) {
    EXPECT_EQ(values[index], expected_variants[index])
        << "Vectors int_value_segment.values() and expected_variants differ at index " << index;
  }
}

}  // namespace opossum
