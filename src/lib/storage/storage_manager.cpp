#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) { _tables[name] = table; }

void StorageManager::drop_table(const std::string& name) {
  // TODO: Any simpler method?
  if (_tables.at(name)) {
    _tables.erase(name);
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (auto const& table : _tables) {
    names.push_back(table.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto it = _tables.begin(); it != _tables.end(); ++it) {
    out << "Name: " << it->first << ", ";
    out << "#columns: " << it->second->column_count() << ", ";
    out << "#rows: " << it->second->row_count() << ", ";
    out << "#chunks: " << it->second->chunk_count() << ", ";
    out << std::endl;
  }
}

void StorageManager::reset() { _tables.clear(); }

}  // namespace opossum
