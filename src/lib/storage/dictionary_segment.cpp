#include "dictionary_segment.hpp"
#include <algorithm>
#include <set>
#include "fixed_width_attribute_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<AbstractSegment>& abstract_segment) {
  Assert(abstract_segment->size() > 0, "Input segment must contain values.");

  const auto value_segment = std::static_pointer_cast<ValueSegment<T>>(abstract_segment);
  std::set<T> distinct_values;

  for (const auto& value : value_segment->values()) {
    distinct_values.insert(value);
  }

  // Populate the _dictionary with the unique values.
  _dictionary.reserve(distinct_values.size());
  std::copy(distinct_values.begin(), distinct_values.end(), std::back_inserter(_dictionary));

  // Initialize the _attribute_vector based on the number of unique values.
  if (_dictionary.size() < std::numeric_limits<uint8_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthAttributeVector<uint8_t>>(value_segment->size());
  } else if (_dictionary.size() < std::numeric_limits<uint16_t>::max()) {
    _attribute_vector = std::make_shared<FixedWidthAttributeVector<uint16_t>>(value_segment->size());
  } else {
    _attribute_vector = std::make_shared<FixedWidthAttributeVector<uint32_t>>(value_segment->size());
  }

  // Populate the _attribute_vector with the offsets.
  for (size_t i = 0; i < value_segment->size(); ++i) {
    auto find_iterator = std::find(_dictionary.cbegin(), _dictionary.cend(), value_segment->values()[i]);
    _attribute_vector->set(i, (ValueID)std::distance(_dictionary.cbegin(), find_iterator));
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
  Assert(dictionary_offset <= _dictionary.size(), "");
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
  auto lower_bound = std::lower_bound(_dictionary.begin(), _dictionary.end(), value);
  return lower_bound != _dictionary.end() ? (ValueID)std::distance(_dictionary.begin(), lower_bound) : INVALID_VALUE_ID;
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  auto lower_bound = std::lower_bound(_dictionary.begin(), _dictionary.end(), type_cast<T>(value));
  return lower_bound != _dictionary.end() ? (ValueID)std::distance(_dictionary.begin(), lower_bound) : INVALID_VALUE_ID;
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const T value) const {
  auto upper_bound = std::upper_bound(_dictionary.begin(), _dictionary.end(), value);
  return upper_bound != _dictionary.end() ? (ValueID)std::distance(_dictionary.begin(), upper_bound) : INVALID_VALUE_ID;
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  auto upper_bound = std::upper_bound(_dictionary.begin(), _dictionary.end(), type_cast<T>(value));
  return upper_bound != _dictionary.end() ? (ValueID)std::distance(_dictionary.begin(), upper_bound) : INVALID_VALUE_ID;
}

template <typename T>
ChunkOffset DictionarySegment<T>::unique_values_count() const {
  return (ChunkOffset)_dictionary.size();
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return (ChunkOffset)_attribute_vector->size();
}

template <typename T>
size_t DictionarySegment<T>::estimate_memory_usage() const {
  return sizeof(T) * _dictionary.size() + _attribute_vector->width() * _attribute_vector->size();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
