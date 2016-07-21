#ifndef TINY_BASE_TABLE_MANAGER_H_
#define TINY_BASE_TABLE_MANAGER_H_

#include <cstdint>
#include <string>
#include <vector>
#include "file_util.h"
#include "page_manager.h"
#include "sql_command.h"

namespace internal {

using PrimaryKey = CellKey;
using TableSchema = sql::CreateTableCommand;

class TableManager {
 public:
  /* Let class get ready */
  TableManager(const fs::path& file_path);

  ~TableManager(void);

  bool Exists(void) { return fs::exists(file_path_); }

  void Load(const TableSchema& schema);

  /* Physically create table and save schema */
  void CreateTable(const sql::CreateTableCommand& command);

  void InsertInto(const sql::InsertIntoCommand& command);

 private:
  // info for the table
  fs::path file_path_;
  PageIndex root_page_;
  uint32_t page_num_;
  std::vector<PageManager> page_list_;

  // B plus tree
  uint8_t fanout_;

  // tool
  utils::FileHandle table_file_;

  // sql
  TableSchema table_schema_;

  // page
  void CreatePage(const PageType& page_type);

  void LoadPage(void);

  // TODO: considering merge them into command class (abstract class)
  PrimaryKey GetPrimaryKey(const sql::InsertIntoCommand& command);

  PageRecord PrepareRecord(const sql::InsertIntoCommand& command);

  // B Plus Tree
  void InsertRecord(const PrimaryKey& primary_key, const PageRecord& record);

  void InsertRecord_(const PageIndex& page_index, const PrimaryKey& primary_key,
                     const PageRecord& record);

  PageIndex SearchPage(const PageIndex& current_page,
                       const PrimaryKey& primary_key);

  void UpdateFanout(const PageIndex& page_index);

  bool IsOverflow(const PageIndex& page_index) const;

  bool IsRoot(const PageIndex& page_index) const {
    return (page_index == root_page_);
  }

  bool IsLeaf(const PageIndex& page_index) const {
    return page_list_[page_index].IsLeaf();
  }

  bool HasSpace(const PageIndex& page_index,
                const utils::FileOffset& record_size) const {
    return page_list_[page_index].HasSpace(record_size);
  }
};

}  // namespace page

#endif  // TINY_BASE_TABLE_MANAGER_H_
