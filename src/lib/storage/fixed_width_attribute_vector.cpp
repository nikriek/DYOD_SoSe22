#include "fixed_width_attribute_vector.hpp"
#include "all_type_variant.hpp"
namespace opossum {

template <typename T>
FixedWidthAttributeVector<T>::FixedWidthAttributeVector(const size_t size) {
  _values.reserve(size);
}

template <typename T>
ValueID FixedWidthAttributeVector<T>::get(const size_t index) const {
  return static_cast<ValueID>(_values.at(index));
}

template <typename T>
void FixedWidthAttributeVector<T>::set(const size_t index, const ValueID value_id) {
  if (_values.size() <= index) {
    _values.push_back(value_id);
  } else {
    _values[index] = value_id;
  }
}

template <typename T>
size_t FixedWidthAttributeVector<T>::size() const {
  return _values.size();
}

template <typename T>
AttributeVectorWidth FixedWidthAttributeVector<T>::width() const {
  return sizeof(T);
}

template class FixedWidthAttributeVector<uint32_t>;
template class FixedWidthAttributeVector<uint16_t>;
template class FixedWidthAttributeVector<uint8_t>;

}  // namespace opossum
