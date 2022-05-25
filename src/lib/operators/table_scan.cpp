#include "table_scan.hpp"
#include "resolve_type.hpp"
#include "type_cast.hpp"
#include "storage/table.hpp"
#include "storage/chunk.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                     const ScanType scan_type, const AllTypeVariant search_value)
    : AbstractOperator(in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

ColumnID TableScan::column_id() const { return _column_id; }

ScanType TableScan::scan_type() const { return _scan_type; }

const AllTypeVariant& TableScan::search_value() const { return _search_value; }

  // for each chunk
  // pos_list = scan_chunk()
  // ref_segment = make_shared<ReferenceSegment>(pos_list);
  // chunks.push_back(chunk(ref_segment));
  // table->SetChunks(chunks)
  // return table;

  // get the column
  // get chunks
  // for chunk in chunk get segment type
  // compare helper method? -> check for encoding & scan
std::shared_ptr<const Table> TableScan::_on_execute() {
    const auto input_table = _left_input->get_output();
    const auto target_chunk_size = input_table->target_chunk_size();
    const auto table = std::make_shared<Table>(target_chunk_size);
    const auto chunk_count = input_table->chunk_count();

    for (ChunkID index{0}; index < chunk_count; ++index) {
        auto chunk = input_table->get_chunk(index);
        auto data_type = input_table->column_type(_column_id);

        resolve_data_type(data_type, [&] (auto type) {
            using Type = typename decltype(type)::type;
            auto segment = chunk->get_segment(_column_id);
            // TODO dictionary segment
            const auto typed_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
            
            auto comparator = get_comparator<Type>(_scan_type);
            bool test = comparator(2, 3);
            //const auto result = scan_value_segment<Type>(typed_segment, comparator);
        });
    }
    return nullptr;

}

template <typename T, typename Comparator>
PosList TableScan::scan_value_segment(std::shared_ptr<ValueSegment<T>>& segment, Comparator comparator) {

    const auto search_value = type_cast<T>(_search_value);
    auto const values = segment->values();
    for (ChunkOffset index{0}; index < segment->size(); ++index) {
       bool should_emit = comparator(values[index], search_value);
    }

    return PosList();
}

template <typename T, typename Comparator>
PosList TableScan::scan_dictionary_segment(std::shared_ptr<ValueSegment<T>>& segment, Comparator comparator) {
    return PosList();
}

template <typename T> 
std::binary_function<T,T,bool> TableScan::get_comparator(ScanType scan_type) {
    switch (scan_type) {
    case ScanType::OpEquals:
        return std::equal_to<T>{};
    case ScanType::OpNotEquals:
        return std::not_equal_to<T>{};
    case ScanType::OpLessThan:
        return std::less<T>{};
    case ScanType::OpLessThanEquals:
        return std::less_equal<T>{};
    case ScanType::OpGreaterThan:
        return std::greater<T>{};
    case ScanType::OpGreaterThanEquals:
        return std::greater_equal<T>{};
    } 
    
    return std::greater_equal<T>{};
}
}  // namespace opossum