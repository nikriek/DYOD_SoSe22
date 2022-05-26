#include <utility>
#include <functional>

#include "table_scan.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "storage/table.hpp"
#include "storage/chunk.hpp"
#include "utils/assert.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto input_table = _left_input->get_output();
  const auto target_chunk_size = input_table->target_chunk_size();
  const auto output_table = std::make_shared<Table>(target_chunk_size);
  const auto chunk_count = input_table->chunk_count();
  std::vector<std::shared_ptr<Chunk>> output_chunks;
  output_chunks.reserve(input_table->chunk_count());

  // TODO(anyone): Add assertion for types

  for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(chunk_id);
    auto data_type = input_table->column_type(_column_id);

    // TODO(anyone): Move type resolvement someehwre outside of this loop
    resolve_data_type(data_type, [&](auto type) {
      using Type = typename decltype(type)::type;
      auto segment = chunk->get_segment(_column_id);
      // TODO(anyone): dictionary segment
      // TODO(anyone): Grow chunk based on max_chunk_size
      const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
      resolve_comparator<Type>(_scan_type, [&](auto comparator) {
        auto position_list = scan_value_segment<Type>(value_segment, comparator, chunk_id);
        auto output_chunk = std::make_shared<Chunk>();
        output_chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, _column_id, position_list));
        output_chunks.push_back(output_chunk);
      });
    });
  }
  std::vector<std::string> column_types;
  column_types.reserve(input_table->column_names().size());

  // TODO(anyone): add a getter for column_types to table to avoid this senseless copy
  for (ColumnID index{0}; index < input_table->column_names().size(); ++index) {
    column_types.push_back(input_table->column_type(index));
  }
  return std::make_shared<Table>(std::move(output_chunks), input_table->column_names(), column_types);
}

template <typename T, typename Comparator>
std::shared_ptr<PosList> TableScan::scan_value_segment(const std::shared_ptr<ValueSegment<T>>& segment,
                                                       Comparator comparator, const ChunkID chunk_id) {
  auto position_list = std::make_shared<PosList>();
  position_list->reserve(segment->size()); 

  const auto search_value = type_cast<T>(_search_value);
  auto const values = segment->values();
  for (ChunkOffset index{0}; index < segment->size(); ++index) {
    bool should_emit = comparator(values[index], search_value);
    if (should_emit) {
      position_list->emplace_back(RowID{chunk_id, index});
    }
  }

  return position_list;
}

template <typename T, typename Comparator>
std::shared_ptr<PosList> TableScan::scan_value_segment_optimized(const std::shared_ptr<ValueSegment<T>>& segment,
                                                            Comparator comparator, const ChunkID chunk_id) {
  auto position_list_ptr = std::make_shared<PosList>();
  position_list_ptr->reserve(segment->size()); 

  const auto search_value = type_cast<T>(_search_value);
  auto const values = segment->values();

  PosList position_list = *(position_list_ptr);
  ChunkOffset insertion_index{0};
  for (ChunkOffset index{0}; index < segment->size(); ++index) {
    position_list[insertion_index] = (RowID{chunk_id, index});
    insertion_index += comparator(values[index], search_value);
  }

  position_list_ptr->shrink_to_fit();
  return position_list_ptr;                                          
}


template <typename T, typename Comparator>
PosList TableScan::scan_dictionary_segment(std::shared_ptr<ValueSegment<T>>& segment, Comparator comparator) {
  return PosList();
}

template <typename T, typename Functor>
void TableScan::resolve_comparator(ScanType scan_type, const Functor& func) {
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
