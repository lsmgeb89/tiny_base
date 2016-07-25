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
using CellPivot = std::pair<CellIndex, CellKey>;
using TableSchema = sql::CreateTableCommand;

class TableManager {
 public:
  /* Let class get ready */
  TableManager(const fs::path& file_path);

  ~TableManager(void);

  bool Exists(void) { return fs::exists(file_path_); }

  void Load(const TableSchema& schema, const uint8_t& fanout,
            const PageIndex& root_page);

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
  PageIndex CreatePage(const PageType& page_type);

  void LoadPage(void);

  PageIndex SplitLeafPage(const PageIndex& target_page,
                          const CellIndex& cell_index,
                          const PrimaryKey& primary_key, const PageCell& cell);

  PageIndex SplitInteriorPage(const PageIndex& target_page,
                              const CellPivot& cell_pivot,
                              const PrimaryKey& primary_key,
                              const PageCell& cell,
                              std::shared_ptr<PageIndex> right_most_pointer);

  // TODO: considering merge them into command class (abstract class)
  PrimaryKey GetPrimaryKey(const sql::InsertIntoCommand& command);

  // for leaf pages
  PageCell PrepareLeafCell(const sql::InsertIntoCommand& command);

  // for interior pages
  PageCell PrepareInteriorCell(const int32_t& left_pointer, const int32_t& key);

  // B Plus Tree
  void InsertCell(const PageIndex& target_page, const PrimaryKey& primary_key,
                  const PageCell& cell,
                  std::shared_ptr<PageIndex> right_most_pointer);

  void DoInsertCell(const PageIndex& page_index, const PrimaryKey& primary_key,
                    const PageCell& cell);

  PageIndex SearchPage(const PageIndex& current_page,
                       const PrimaryKey& primary_key);

  void UpdateFanout(const PageIndex& page_index);

  bool WillOverflow(const PageIndex& page_index) const;

  bool IsRoot(const PageIndex& page_index) const {
    return (page_index == root_page_);
  }

  // wrappers
  CellPivot GetCellPivot(const PageIndex& page_index, const CellKey& cell_key);

  PageIndex GetParent(const PageIndex& page_index);

  CellKey GetCellKey(const PageIndex& page_index, const CellIndex& cell_index) {
    return page_list_[page_index].GetCellKey(cell_index);
  }

  PagePointer GetRightMostPointer(const PageIndex& page_index) {
    return page_list_[page_index].GetRightMostPagePointer();
  }

  void SetRightMostPointer(const PageIndex& page_index,
                           const PagePointer& right_most_pointer) {
    return page_list_[page_index].SetPageRightMostPointer(right_most_pointer);
  }

  bool IsLeaf(const PageIndex& page_index) const {
    return page_list_[page_index].IsLeaf();
  }

  bool HasSpace(const PageIndex& page_index,
                const utils::FileOffset cell_size) const {
    return page_list_[page_index].HasSpace(cell_size);
  }

  void SetParent(const PageIndex& page_index, const PageIndex& parent_index) {
    return page_list_[page_index].SetParent(parent_index);
  }

  void UpdateParent(const PageIndex& page_index);

  void LoadParent(const PageIndex& page_index);
};

}  // namespace page

#endif  // TINY_BASE_TABLE_MANAGER_H_