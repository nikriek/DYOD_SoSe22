#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "dictionary_segment.hpp"
#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const ChunkOffset target_chunk_size) : _target_chunk_size(target_chunk_size) { create_new_chunk(); }

void Table::add_column(const std::string& name, const std::string& type) {
  // We only allow adding columns for empty tables to avoid dealing with default values
  Assert(row_count() == 0, "Adding a column is only allowed for empty tables");
  _column_names.push_back(name);
  _column_types.push_back(type);
  resolve_data_type(type, [&_chunks = _chunks](auto type) {
    using Type = typename decltype(type)::type;
    _chunks.back()->add_segment(std::make_shared<ValueSegment<Type>>());
  });
}

void Table::append(const std::vector<AllTypeVariant>& values) {
  if (_chunks.back()->size() >= _target_chunk_size) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  auto new_chunk = std::make_shared<Chunk>();
  _chunks.push_back(new_chunk);
  for (const auto& type : _column_types) {
    resolve_data_type(type, [&new_chunk](auto type) {
      using Type = typename decltype(type)::type;
      new_chunk->add_segment(std::make_shared<ValueSegment<Type>>());
    });
  }
}

ColumnCount Table::column_count() const { return static_cast<ColumnCount>(_column_names.size()); }

ChunkOffset Table::row_count() const {
  // Calculate the count of all full chunks multiplied by the target/max chunk size
  // and then add the size of the last, incomplete chunk
  // Since our knowledge about chunks is still limited, we assume that this is the right
  // method for now. This is up to change later.
  return ChunkOffset{(chunk_count() - 1) * _target_chunk_size + _chunks.back()->size()};
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Since this method is only used for debugging, we are fine with a linear
  // search in the column name list. Otherwise, we would need different
  // data structures which are not useful and have overhead in a prod release.
  // Linear search shouldn't be distinguishably slow on small vectors, anyways
  const auto pos =
      std::distance(_column_names.cbegin(), std::find(_column_names.cbegin(), _column_names.cend(), column_name));
  DebugAssert(pos < column_count(), "Column name was not found");
  return static_cast<ColumnID>(pos);
}

ChunkOffset Table::target_chunk_size() const { return _target_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(const ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(const ColumnID column_id) const { return _column_types.at(column_id); }

std::shared_ptr<Chunk> Table::get_chunk(ChunkID chunk_id) { return _chunks.at(chunk_id); }

std::shared_ptr<const Chunk> Table::get_chunk(ChunkID chunk_id) const { return _chunks.at(chunk_id); }

void Table::compress_chunk(const ChunkID chunk_id) {
  const auto input_chunk = get_chunk(chunk_id);
  const auto column_count = input_chunk->column_count();
  std::vector<std::thread> threads;
  threads.reserve(column_count);
  const auto compressed_chunk = std::make_shared<Chunk>();
  std::vector<std::shared_ptr<AbstractSegment>> compressed_segments(column_count);

  for (ColumnID index{0}; index < column_count; ++index) {
    const auto segment = input_chunk->get_segment(index);
    threads.emplace_back([&, index, segment] {
      resolve_data_type(_column_types[index], [&](auto type) {
        using DataType = typename decltype(type)::type;
        compressed_segments[index] = std::make_shared<DictionarySegment<DataType>>(segment);
      });
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (size_t index = 0; index < column_count; ++index) {
    compressed_chunk->add_segment(compressed_segments[index]);
  }
  _chunks[chunk_id] = compressed_chunk;
}

}  // namespace opossum
