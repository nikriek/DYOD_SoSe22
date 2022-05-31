#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ColumnID column_id() const;

  ScanType scan_type() const;

  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

 private:
  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;

  template <typename T, typename Comparator>
  void scan_value_segment(const std::shared_ptr<ValueSegment<T>> segment, const T search_value, Comparator comparator,
                          const ChunkID chunk_id, std::shared_ptr<PosList> position_list);

  template <typename T, typename Comparator>
  void scan_dictionary_segment(std::shared_ptr<DictionarySegment<T>> segment, const T search_value,
                               Comparator comparator, const ChunkID chunk_id, std::shared_ptr<PosList>& position_list);

  template <typename T, typename Comparator>
  std::shared_ptr<const Table> scan_reference_segment(std::shared_ptr<ReferenceSegment> segment, const T search_value, Comparator comparator,
                                std::shared_ptr<PosList> position_list);
};

}  // namespace opossum
