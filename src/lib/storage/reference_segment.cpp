#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const std::shared_ptr<const PosList>& pos)
    : _referenced_table(referenced_table), _referenced_column_id(referenced_column_id), _position_list(pos) {}

ChunkOffset ReferenceSegment::size() const { return _position_list->size(); }

size_t ReferenceSegment::estimate_memory_usage() const {
  // TODO(theo): revisit
  return sizeof(RowID) * size();
}

const std::shared_ptr<const PosList>& ReferenceSegment::pos_list() const { return _position_list; }

ColumnID ReferenceSegment::referenced_column_id() const { return _referenced_column_id; }

const std::shared_ptr<const Table>& ReferenceSegment::referenced_table() const { return _referenced_table; }

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  RowID row_id = (*_position_list)[chunk_offset];
  return (*_referenced_table->get_chunk(row_id.chunk_id)->get_segment(_referenced_column_id))[row_id.chunk_offset];
}

}  // namespace opossum
