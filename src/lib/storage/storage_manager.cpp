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
  // Throws an exception if the key is not found
  Assert(_tables.contains(name), "Should contain table with given name");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.contains(name); }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  names.reserve(_tables.size());
  for (const auto& [table_name, _] : _tables) {
    names.push_back(table_name);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto it = _tables.begin(), end = _tables.end(); it != end; ++it) {
    out << "Name: " << it->first << ", ";
    out << "#columns: " << it->second->column_count() << ", ";
    out << "#rows: " << it->second->row_count() << ", ";
    out << "#chunks: " << it->second->chunk_count();
    out << std::endl;
  }
}

void StorageManager::reset() { 
  // The documentation says "Deletes the entire StorageManager and creates a new one, 
  // used especially in tests.". Clearing the tables does reset the data, but keeps
  // the address of the singleton. Resetting a static doesn't seem to be possible
  _tables.clear();
}

}  // namespace opossum
