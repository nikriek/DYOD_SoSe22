#pragma once

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include <boost/hana/equal.hpp>
#include <boost/hana/for_each.hpp>
#include <boost/hana/size.hpp>

#include "all_type_variant.hpp"
#include "utils/assert.hpp"

#include "storage/value_segment.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * Resolves a type string by passing a hana::type object on to a generic lambda
 *
 * @param type_string is a string representation of any of the supported data types
 * @param func is a generic lambda or similar accepting a hana::type object
 *
 *
 * Note on hana::type (taken from Boost.Hana documentation):
 *
 * For subtle reasons having to do with ADL, the actual representation of hana::type is
 * implementation-defined. In particular, hana::type may be a dependent type, so one
 * should not attempt to do pattern matching on it. However, one can assume that hana::type
 * inherits from hana::basic_type, which can be useful when declaring overloaded functions.
 *
 * This means that we need to use hana::basic_type as a parameter in methods so that the
 * underlying type can be deduced from the object.
 *
 *
 * Note on generic lambdas (taken from paragraph 5.1.2/5 of the C++14 Standard Draft n3690):
 *
 * For a generic lambda, the closure type has a public inline function call operator member template (14.5.2)
 * whose template-parameter-list consists of one invented type template-parameter for each occurrence of auto
 * in the lambda’s parameter-declaration-clause, in order of appearance. Example:
 *
 *   auto lambda = [] (auto a) { return a; };
 *
 *   class // unnamed {
 *    public:
 *     template<typename T>
 *     auto operator()(T a) const { return a; }
 *   };
 *
 *
 * Example:
 *
 *   template <typename T>
 *   process_variant(const T& var);
 *
 *   template <typename T>
 *   process_type(hana::basic_type<T> type);  // note: parameter type needs to be hana::basic_type not hana::type!
 *
 *   resolve_data_type(type_string, [&](auto type) {
 *     using Type = typename decltype(type)::type;
 *     const auto var = type_cast<Type>(variant_from_elsewhere);
 *     process_variant(var);
 *
 *     process_type(type);
 *   });
 */
template <typename Functor>
void resolve_data_type(const std::string& type_string, const Functor& func) {
  hana::for_each(data_types, [&](auto x) {
    if (std::string(hana::first(x)) == type_string) {
      // The + before hana::second - which returns a reference - converts its return value into a value
      func(+hana::second(x));
      return;
    }
  });
}

/**
 * @brief Resolve a type which the size value can fit into. Types are evaluated for their numeric max limit
 * in the given order from the template parameters. 
 * 
 * Example:
 * 
 * resolve_fixed_width_integer_type<uint8_t, uint16_t, uint32_t>(size, [&](auto type){
 *   using DataType = typename decltype(type)::type;
 *   _attribute_vector = std::make_shared<FixedWidthAttributeVector<DataType>>(size);
 * });
 * 
 * @tparam TypeHead First type to evaluate
 * @tparam TypesTail Other types to recursively evaluate
 * @tparam SizeType Type of size to check 
 * @tparam Functor Evaluation callback called with type boost::hana::type_c
 * @param size Input size to fit in given data types
 * @param func Function to call with a boost::hana::type_c
 */
template <typename TypeHead, typename... TypesTail, typename SizeType, typename Functor>
void resolve_fixed_width_integer_type(const SizeType size, const Functor& func) {
  if (size <= std::numeric_limits<TypeHead>::max()) {
    func(boost::hana::type_c<TypeHead>);
    return;
  }

  if constexpr (sizeof...(TypesTail) > 0) {
    resolve_fixed_width_integer_type<TypesTail...>(size, func);
  } else {
    Fail("Could not resolve fixed width integer type for given size");
  }
}

template <typename T, typename Functor>
void resolve_comparator(ScanType scan_type, const Functor& func) {
  switch (scan_type) {
    case ScanType::OpEquals:
      func(std::equal_to<T>{});
      break;
    case ScanType::OpNotEquals:
      func(std::not_equal_to<T>{});
      break;
    case ScanType::OpLessThan:
      func(std::less<T>{});
      break;
    case ScanType::OpLessThanEquals:
      func(std::less_equal<T>{});
      break;
    case ScanType::OpGreaterThan:
      func(std::greater<T>{});
      break;
    case ScanType::OpGreaterThanEquals:
      func(std::greater_equal<T>{});
      break;
    default:
      Fail("Invalid scan type.");
  }
}

}  // namespace opossum
