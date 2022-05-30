#include <memory>
#include <sstream>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"

namespace opossum {

class StorageStorageManagerTest : public BaseTest {
 protected:
  void SetUp() override {
    auto& storage_manager = StorageManager::get();
    auto table_a = std::make_shared<Table>();
    auto table_b = std::make_shared<Table>(4);

    storage_manager.add_table("first_table", table_a);
    storage_manager.add_table("second_table", table_b);
  }
};

TEST_F(StorageStorageManagerTest, GetTable) {
  auto& storage_manager = StorageManager::get();
  auto table_c = storage_manager.get_table("first_table");
  auto table_d = storage_manager.get_table("second_table");
  EXPECT_THROW(storage_manager.get_table("third_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DropTable) {
  auto& storage_manager = StorageManager::get();
  storage_manager.drop_table("first_table");
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
  EXPECT_THROW(storage_manager.drop_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, ResetTable) {
  StorageManager::get().reset();
  auto& storage_manager = StorageManager::get();
  EXPECT_THROW(storage_manager.get_table("first_table"), std::exception);
}

TEST_F(StorageStorageManagerTest, DoesNotHaveTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("third_table"), false);
}

TEST_F(StorageStorageManagerTest, HasTable) {
  auto& storage_manager = StorageManager::get();
  EXPECT_EQ(storage_manager.has_table("first_table"), true);
}

TEST_F(StorageStorageManagerTest, TableNames) {
  auto& storage_manager = StorageManager::get();
  auto table_names = storage_manager.table_names();
  // The insertion order is different to return value order due to hashing
  auto expected_table_names = std::vector<std::string>{"second_table", "first_table"};

  ASSERT_EQ(table_names.size(), expected_table_names.size())
      << "Vectors table_names and expected_table_names are of unequal length";
  for (size_t index = 0; index < table_names.size(); ++index) {
    EXPECT_EQ(table_names[index], expected_table_names[index])
        << "Vectors table_names and expected_table_names differ at index " << index;
  }
}

TEST_F(StorageStorageManagerTest, Print) {
  auto& storage_manager = StorageManager::get();
  auto table = storage_manager.get_table("first_table");
  table->add_column("column_1", "int");
  table->add_column("column_2", "float");
  table->append({1, 1.4});
  table->append({2, 4.4});
  table->append({3, 7.4});

  std::stringstream out;
  storage_manager.print(out);
  EXPECT_EQ(
      out.str(),
      "Name: second_table, #columns: 0, #rows: 0, #chunks: 1\nName: first_table, #columns: 2, #rows: 3, #chunks: 1\n");
}

}  // namespace opossum
