#include "value_segment.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

template <typename T>
AllTypeVariant ValueSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  return AllTypeVariant(this->data[chunk_offset]);
}

template <typename T>
void ValueSegment<T>::append(const AllTypeVariant& val) {
  // TODO: Max size of vector
  auto containedValue = type_cast<T>(val);
  this->data.push_back(containedValue);
}

template <typename T>
ChunkOffset ValueSegment<T>::size() const {
  return this->data.size();
}

template <typename T>
const std::vector<T>& ValueSegment<T>::values() const {
  return data;
}

template <typename T>
size_t ValueSegment<T>::estimate_memory_usage() const {
  return sizeof(T) * this->size();
}

// Macro to instantiate the following classes:
// template class ValueSegment<int32_t>;
// template class ValueSegment<int64_t>;
// template class ValueSegment<float>;
// template class ValueSegment<double>;
// template class ValueSegment<std::string>;
EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueSegment);

}  // namespace opossum
