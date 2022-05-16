#include <cmath>
#include <memory>
#include <string>
#include "../base_test.hpp"

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_width_attribute_vector.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int32_t>> value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  std::shared_ptr<ValueSegment<std::string>> value_segment_str = std::make_shared<ValueSegment<std::string>>();
  std::shared_ptr<DictionarySegment<std::string>> _string_dict_segment;

  void SetUp() override {
    value_segment_str->append("Bill");
    value_segment_str->append("Steve");
    value_segment_str->append("Alexander");
    value_segment_str->append("Steve");
    value_segment_str->append("Hasso");
    value_segment_str->append("Bill");

    std::shared_ptr<AbstractSegment> segment;
    resolve_data_type("string", [&](auto type) {
      using Type = typename decltype(type)::type;
      segment = std::make_shared<DictionarySegment<Type>>(value_segment_str);
    });

    _string_dict_segment = std::dynamic_pointer_cast<DictionarySegment<std::string>>(segment);
  }
};

TEST_F(StorageDictionarySegmentTest, CompressSegmentString) {
  // Test attribute_vector size
  EXPECT_EQ(_string_dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(_string_dict_segment->unique_values_count(), 4u);

  // Test sorting
  const auto dict = _string_dict_segment->dictionary();
  EXPECT_EQ(dict[0], "Alexander");
  EXPECT_EQ(dict[1], "Bill");
  EXPECT_EQ(dict[2], "Hasso");
  EXPECT_EQ(dict[3], "Steve");

  const auto attribute_vector = _string_dict_segment->attribute_vector();
  EXPECT_EQ(attribute_vector->get(ChunkOffset{0}), 1);
  EXPECT_EQ(attribute_vector->get(ChunkOffset{1}), 3);
  EXPECT_EQ(attribute_vector->get(ChunkOffset{2}), 0);
  EXPECT_EQ(attribute_vector->get(ChunkOffset{3}), 3);
  EXPECT_EQ(attribute_vector->get(ChunkOffset{4}), 2);
  EXPECT_EQ(attribute_vector->get(ChunkOffset{5}), 1);
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (auto value = int16_t{0}; value <= 10; value += 2) {
    value_segment_int->append(value);
  }

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);

  EXPECT_EQ(dict_segment->lower_bound(4), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(4), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant{4}), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant{4}), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(5), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(5), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(15), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(15), INVALID_VALUE_ID);

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant{15}), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant{15}), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, Append) {
  value_segment_int->append(2);

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);
  EXPECT_THROW(dict_segment->append(2), std::exception);
}

TEST_F(StorageDictionarySegmentTest, EstimateMemoryUsage) {
  value_segment_int->append(2);
  value_segment_int->append(3);
  value_segment_int->append(3);
  value_segment_int->append(3);
  value_segment_int->append(3);
  value_segment_int->append(3);

  std::shared_ptr<AbstractSegment> segment;
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int32_t>>(segment);
  // 4 bytes for int * 2 distinct values + 1 byte * 6 values in total
  EXPECT_EQ(dict_segment->estimate_memory_usage(), 14u);
}

TEST_F(StorageDictionarySegmentTest, Access) {
  std::string expected_value = "Bill";
  EXPECT_EQ((*_string_dict_segment)[0], AllTypeVariant{expected_value});
  EXPECT_EQ(_string_dict_segment->get(0), expected_value);
}

TEST_F(StorageDictionarySegmentTest, ValueOfValueId) {
  EXPECT_EQ(_string_dict_segment->value_of_value_id((ValueID)0), "Alexander");
}

class StorageDictionarySegmentAdaptiveAttributeVectorSizeTest
    : public ::testing::TestWithParam<std::tuple<int64_t, int64_t>> {
 protected:
  std::shared_ptr<ValueSegment<int32_t>> value_segment_int = std::make_shared<ValueSegment<int32_t>>();
  std::shared_ptr<AbstractSegment> segment;
};

TEST_P(StorageDictionarySegmentAdaptiveAttributeVectorSizeTest, EstimateMemoryUsage) {
  int distinct_value_count = std::get<0>(GetParam());
  int expected_memory_usage = std::get<1>(GetParam());

  for (auto index = int32_t{0}; index < distinct_value_count; ++index) {
    value_segment_int->append(index);
  }
  resolve_data_type("int", [&](auto type) {
    using Type = typename decltype(type)::type;
    segment = std::make_shared<DictionarySegment<Type>>(value_segment_int);
  });

  EXPECT_EQ(segment->estimate_memory_usage(), expected_memory_usage);
}

INSTANTIATE_TEST_SUITE_P(EstimateMemoryUsage, StorageDictionarySegmentAdaptiveAttributeVectorSizeTest,
                         ::testing::Values(std::make_tuple(std::pow(2, 4), 80),           // uint8_t
                                           std::make_tuple(std::pow(2, 8) - 1, 1275),     // uint8_t
                                           std::make_tuple(std::pow(2, 8), 1536),         // uint16_t
                                           std::make_tuple(std::pow(2, 10), 6144),        // uint16_t
                                           std::make_tuple(std::pow(2, 16) - 1, 393210),  // uint16_t
                                           std::make_tuple(std::pow(2, 16), 524288),      // uint32_t
                                           std::make_tuple(std::pow(2, 19), 4194304)));   // uint32_t

}  // namespace opossum
