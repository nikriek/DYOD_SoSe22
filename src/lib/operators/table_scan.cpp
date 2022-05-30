#include <functional>
#include <utility>

#include "resolve_type.hpp"
#include "storage/chunk.hpp"
#include "storage/fixed_width_attribute_vector.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "table_scan.hpp"
#include "type_cast.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

std::shared_ptr<const Table> TableScan::_on_execute() {
  const auto input_table = _left_input->get_output();
  const auto output_table = std::make_shared<Table>();
  const auto column_count = input_table->column_count();

  // If we receive an empty chunk,
  // we return an empty output table with one chunk only and return early
  if (input_table->row_count() == 0) {
    for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
      output_table->add_column_definition(input_table->column_name(column_id), input_table->column_type(column_id));
    }
    return output_table;
  }

  auto position_list = std::make_shared<PosList>();
  position_list->reserve(input_table->row_count());
  const auto chunk_count = input_table->chunk_count();
  std::shared_ptr<Chunk> output_chunk = output_table->get_chunk(ChunkID{0});

  for (ColumnID column_id{0}; column_id < column_count; ++column_id) {
    output_table->add_column_definition(input_table->column_name(column_id), input_table->column_type(column_id));
    output_chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, column_id, position_list));
  }

  // Use the first chunk to configure the datatype
  auto data_type = input_table->column_type(_column_id);
  resolve_data_type(data_type, [&](auto type) {
    using Type = typename decltype(type)::type;
    // TODO(anyone): Do we need an assert?!
    const auto search_value = type_cast<Type>(_search_value);

    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      const auto segment = chunk->get_segment(_column_id);

      if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment)) {
        resolve_comparator<Type>(_scan_type, [&](auto comparator) {
          scan_value_segment<Type>(value_segment, search_value, comparator, chunk_id, position_list);
        });
      } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment)) {
        resolve_comparator<Type>(_scan_type, [&](auto comparator) {
          scan_dictionary_segment<Type>(dictionary_segment, search_value, comparator, chunk_id, position_list);
        });
      } else if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)) {
        resolve_comparator<Type>(_scan_type, [&](auto comparator) {
          scan_reference_segment<Type>(reference_segment, search_value, comparator, chunk_id, position_list);
        });
      } else {
        Fail("Cannot cast the segment for TableScan");
      }
    }

    position_list->shrink_to_fit();
  });

  return output_table;
}

template <typename T, typename Comparator>
void TableScan::scan_value_segment(const std::shared_ptr<ValueSegment<T>> segment, const T search_value,
                                   Comparator comparator, const ChunkID chunk_id,
                                   std::shared_ptr<PosList> position_list) {
  auto const values = segment->values();
  for (ChunkOffset index{0}; index < segment->size(); ++index) {
    bool should_emit = comparator(values[index], search_value);
    if (should_emit) {
      position_list->emplace_back(RowID{chunk_id, index});
    }
  }
}

template <typename T, typename Comparator>
void TableScan::scan_dictionary_segment(std::shared_ptr<DictionarySegment<T>> segment, const T search_value,
                                        Comparator comparator, const ChunkID chunk_id,
                                        std::shared_ptr<PosList>& position_list) {
  if ((segment->lower_bound(search_value) == INVALID_VALUE_ID &&
       (_scan_type == ScanType::OpGreaterThanEquals || _scan_type == ScanType::OpEquals)) ||
      (segment->upper_bound(search_value) == INVALID_VALUE_ID && _scan_type == ScanType::OpGreaterThan)) {
    // 1) There is no value >= search_value in the dictionary, and predicate is one of
    // OpGreaterThanEquals, or OpEquals.
    // OR
    // 2) There is no value > search_value, and predicate is OpGreaterThan.
    // Hence, we can return.
    return;
  }

  // We have to do the actual comparison.
  for (ChunkOffset offset{0}; offset < segment->size(); ++offset) {
    bool should_emit = comparator(segment->get(offset), search_value);
    if (should_emit) {
      position_list->emplace_back(RowID{chunk_id, offset});
    }
  }
}

template <typename T, typename Comparator>
void TableScan::scan_reference_segment(std::shared_ptr<ReferenceSegment> segment, const T search_value,
                                       Comparator comparator, const ChunkID chunk_id,
                                       std::shared_ptr<PosList> position_list_out) {
  const auto position_list = segment->pos_list();
  const auto referenced_table = segment->referenced_table();
  const auto referenced_column_id = segment->referenced_column_id();
  
  for (const RowID& rowID : (*position_list)) {
    auto value = type_cast<T>(
        (*referenced_table->get_chunk(chunk_id)->get_segment(referenced_column_id))[rowID.chunk_offset]);
    bool should_emit = comparator(value, search_value);
    if (should_emit) {
      position_list_out->emplace_back(RowID{chunk_id, rowID.chunk_offset});
    }
  }
}
}  // namespace opossum
