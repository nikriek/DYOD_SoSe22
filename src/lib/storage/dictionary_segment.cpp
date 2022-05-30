#include "dictionary_segment.hpp"
#include <algorithm>
#include <set>
#include "fixed_width_attribute_vector.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  DebugAssert(abstract_segment->size() > 0, "Input segment must contain values.");

  // For now, we can assume to only receive a ValueSegment
  const auto value_segment = std::static_pointer_cast<ValueSegment<T>>(abstract_segment);
  const auto values = value_segment->values();
  std::set<T> distinct_values(values.begin(), values.end());

  // Populate the _dictionary with the unique values.
  _dictionary.reserve(distinct_values.size());
  std::copy(distinct_values.begin(), distinct_values.end(), std::back_inserter(_dictionary));

  // Initialize the _attribute_vector based on the number of unique values.
  const auto value_segment_size = value_segment->size();
  resolve_fixed_width_integer_type<uint8_t, uint16_t, uint32_t>(value_segment_size, [&](auto type) {
    using DataType = typename decltype(type)::type;
    _attribute_vector = std::make_shared<FixedWidthAttributeVector<DataType>>(value_segment_size);
  });

  // Populate the _attribute_vector with the offsets.
  for (size_t index = 0; index < value_segment_size; ++index) {
    // Do binary search to find insert position
    auto find_iterator = std::lower_bound(_dictionary.cbegin(), _dictionary.cend(), values[index]);
    _attribute_vector->set(index, static_cast<ValueID>(std::distance(_dictionary.cbegin(), find_iterator)));
  }
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  const auto dictionary_offset = _attribute_vector->get(chunk_offset);
  return _dictionary[dictionary_offset];
}

template <typename T>
T DictionarySegment<T>::get(const ChunkOffset chunk_offset) const {
  const auto dictionary_offset = _attribute_vector->get(chunk_offset);
  Assert(dictionary_offset <= _dictionary.size(), "cannot find value at given index");
  return _dictionary[dictionary_offset];
}

template <typename T>
void DictionarySegment<T>::append(const AllTypeVariant& value) {
  Fail("Dictionary segments are immutable, i.e., values cannot be appended.");
}

template <typename T>
const std::vector<T>& DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const AbstractAttributeVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const T DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  return _dictionary[value_id];
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const T value) const {
  const auto lower_bound = std::lower_bound(_dictionary.begin(), _dictionary.end(), value);
  if (lower_bound == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), lower_bound));
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  return lower_bound(type_cast<T>(value));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  const auto upper_bound = std::upper_bound(_dictionary.begin(), _dictionary.end(), value);
  if (upper_bound == _dictionary.end()) {
    return INVALID_VALUE_ID;
  }
  return static_cast<ValueID>(std::distance(_dictionary.begin(), upper_bound));
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  return upper_bound(type_cast<T>(value));
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return static_cast<ChunkOffset>(_dictionary.size());
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return sizeof(T) * _dictionary.size() + _attribute_vector->width() * _attribute_vector->size();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
