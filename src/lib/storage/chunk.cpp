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
  this->segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == this->column_count(), "The values to append have the same count as columns");
  for (size_t index = 0; index < values.size(); ++index) {
    this->segments.at(index)->append(values[index]);
  }
}

std::shared_ptr<AbstractSegment> Chunk::get_segment(const ColumnID column_id) const {
  return this->segments.at(column_id);
}

ColumnCount Chunk::column_count() const {
  return ColumnCount{this->segments.size()};
}

ChunkOffset Chunk::size() const {
  if(this->segments.empty()) {
    return 0;
  }
  return this->segments[0]->size();
}

}  // namespace opossum
