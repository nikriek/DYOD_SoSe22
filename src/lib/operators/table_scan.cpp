#include "table_scan.hpp"

namespace opossum {

TableScan::TableScan(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value) {
}

ColumnID TableScan::column_id() const {
    // TODO(student) implement it in a source file and change this to a declaration.
    return ColumnID{};
}

ScanType TableScan::scan_type() const {
    // TODO(student) implement it in a source file and change this to a declaration.
    Fail("Implementation missing.");
}

const AllTypeVariant& TableScan::search_value() const {
    // TODO(student) implement it in a source file and change this to a declaration.
    Fail("Implementation missing.");
}

std::shared_ptr<const Table> TableScan::_on_execute() {
    Fail("Implementation missing.");
}
}