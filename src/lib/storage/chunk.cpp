#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "abstract_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(const std::shared_ptr<AbstractSegment> segment) {
  DebugAssert(std::all_of(_segments.cbegin(), _segments.cend(),
                          [&segment](std::shared_ptr<AbstractSegment> existing_segment) {
                            return existing_segment->size() == segment->size();
                          }),
              "All segments should have the same size");
  _segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == column_count(), "The values to append have the same count as columns");
  for (size_t index = 0; index < values.size(); ++index) {
    _segments.at(index)->append(values[index]);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const { return _segments.at(column_id); }

ColumnCount Chunk::column_count() const { return static_cast<ColumnCount>(_segments.size()); }

ChunkOffset Chunk::size() const {
  if (_segments.empty()) {
    return 0;
  }
  return _segments.front()->size();
}

}  // namespace opossum
