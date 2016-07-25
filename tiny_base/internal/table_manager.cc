#include <algorithm>
#include <cstring>
#include <cassert>
#include <iostream>

#include "endian_util.h"
#include "page_format.h"
#include "table_manager.h"

namespace internal {

TableManager::TableManager(const fs::path& file_path)
    : file_path_(file_path),
      root_page_(0),
      page_num_(0),
      fanout_(std::numeric_limits<decltype(fanout_)>::max()),
      table_file_(std::make_shared<utils::FileUtil>(file_path_)) {}

TableManager::~TableManager(void) {}

void TableManager::Load(const TableSchema& schema, const uint8_t& fanout,
                        const PageIndex& root_page) {
  table_schema_ = schema;
  fanout_ = fanout;
  root_page_ = root_page;

  LoadPage();
  LoadParent(root_page_);
}

void TableManager::CreateTable(const sql::CreateTableCommand& command) {
  if (fs::exists(file_path_)) {
    // TODO: throw error
  }

  // save schema
  table_schema_ = command;

  // create
  fs::create_directories(file_path_.parent_path());
  table_file_->CreateFile();
  CreatePage(TableLeafCell);
}

void TableManager::InsertInto(const sql::InsertIntoCommand& command) {
  CellKey pri_key(GetPrimaryKey(command));
  PageIndex target_page(SearchPage(root_page_, pri_key));
  PageCell cell(PrepareLeafCell(command));

  if ((fanout_ == std::numeric_limits<decltype(fanout_)>::max()) &&
      !HasSpace(target_page, cell.size())) {
    UpdateFanout(target_page);
  }

  InsertCell(target_page, pri_key, cell, nullptr);
}

PrimaryKey TableManager::GetPrimaryKey(const sql::InsertIntoCommand& command) {
  PrimaryKey primary_key =
      std::experimental::any_cast<int32_t>(command.value_list[0]);
  return primary_key;
}

PageCell TableManager::PrepareLeafCell(const sql::InsertIntoCommand& command) {
  std::size_t offset(table_leaf_rowid_offset);
  std::size_t type_size(0);
  int16_t payload_size(0);
  PrimaryKey primary_key(0);
  PageCell table_leaf_cell;

  if (table_schema_.column_list.size() != command.value_list.size()) {
    // TODO: error handling
  }

  primary_key = GetPrimaryKey(command);
  primary_key = utils::SwapEndian<decltype(primary_key)>(primary_key);

  // copy rowid
  table_leaf_cell.resize(table_leaf_rowid_offset + table_leaf_rowid_length);
  std::memcpy(table_leaf_cell.data() + offset, &primary_key,
              table_leaf_rowid_length);
  offset += table_leaf_rowid_length;

  for (auto i = 0; i < table_schema_.column_list.size(); i++) {
    if (table_schema_.column_list[i].type != sql::Text) {
      type_size = sql::DataTypeSize[table_schema_.column_list[i].type];
      table_leaf_cell.resize(table_leaf_cell.size() + type_size);
    }

    switch (table_schema_.column_list[i].type) {
      case sql::OneByteNull:
      case sql::TinyInt:
        table_leaf_cell[offset] =
            std::experimental::any_cast<int8_t>(command.value_list[i]);
        break;
      case sql::TwoByteNull:
      case sql::SmallInt: {
        int16_t value_16 =
            std::experimental::any_cast<int16_t>(command.value_list[i]);
        value_16 = utils::SwapEndian<decltype(value_16)>(value_16);
        std::memcpy(table_leaf_cell.data() + offset, &value_16, type_size);
      } break;
      case sql::FourByteNull:
      case sql::Int: {
        int32_t value_32 =
            std::experimental::any_cast<int32_t>(command.value_list[i]);
        value_32 = utils::SwapEndian<decltype(value_32)>(value_32);
        std::memcpy(table_leaf_cell.data() + offset, &value_32, type_size);
      } break;
      case sql::EightByteNull:
      case sql::BigInt: {
        int64_t value_64 =
            std::experimental::any_cast<int64_t>(command.value_list[i]);
        value_64 = utils::SwapEndian<decltype(value_64)>(value_64);
        std::memcpy(table_leaf_cell.data() + offset, &value_64, type_size);
      } break;
      case sql::Real:
      case sql::Double:
      case sql::DateTime:
      case sql::Date:
        // TODO: implement them
        break;
      case sql::Text: {
        std::string value_str =
            std::experimental::any_cast<std::string>(command.value_list[i]);
        type_size = value_str.size();
        table_leaf_cell.resize(table_leaf_cell.size() + type_size);
        std::reverse(value_str.begin(), value_str.end());
        std::copy(value_str.begin(), value_str.end(),
                  table_leaf_cell.data() + offset);
      } break;
      default:
        break;
    }

    offset += type_size;
  }

  payload_size = table_leaf_cell.size() - table_leaf_payload_offset;
  payload_size = utils::SwapEndian<decltype(payload_size)>(payload_size);
  std::memcpy(table_leaf_cell.data() + table_leaf_payload_length_offset,
              &payload_size, table_leaf_payload_length_length);

  return table_leaf_cell;
}

PageCell TableManager::PrepareInteriorCell(const int32_t& left_pointer,
                                           const int32_t& key) {
  int32_t value_32(0);
  PageCell cell;
  cell.resize(table_interior_cell_length);

  value_32 = utils::SwapEndian<int32_t>(left_pointer);
  std::memcpy(cell.data() + table_interior_left_pointer_offset, &value_32,
              table_interior_left_pointer_length);

  value_32 = utils::SwapEndian<int32_t>(key);
  std::memcpy(cell.data() + table_interior_key_offset, &value_32,
              table_interior_key_length);

  return cell;
}

void TableManager::InsertCell(const PageIndex& target_page,
                              const PrimaryKey& primary_key,
                              const PageCell& cell,
                              std::shared_ptr<PageIndex> right_most_pointer) {
  if (WillOverflow(target_page)) {
    PageIndex parent_page(0);
    PageIndex left_child_page(0);
    std::shared_ptr<PageIndex> right_child_page(std::make_shared<PageIndex>(0));
    CellPivot cell_pivot(GetCellPivot(target_page, primary_key));

    // right split
    PageIndex new_page(0);
    if (IsLeaf(target_page)) {
      new_page =
          SplitLeafPage(target_page, cell_pivot.first, primary_key, cell);
    } else {
      new_page = SplitInteriorPage(target_page, cell_pivot, primary_key, cell,
                                   right_most_pointer);
    }

    if (IsRoot(target_page)) {
      parent_page = CreatePage(TableInteriorCell);
      // update root page
      root_page_ = parent_page;
    } else {
      parent_page = GetParent(target_page);
    }

    // for parent
    left_child_page = target_page;
    *right_child_page = new_page;

    // update parent
    SetParent(left_child_page, parent_page);
    SetParent(*right_child_page, parent_page);

    // bottom up recursion
    InsertCell(parent_page, cell_pivot.second,
               PrepareInteriorCell(left_child_page, cell_pivot.second),
               right_child_page);
  } else {
    if (right_most_pointer) {
      SetRightMostPointer(target_page, *right_most_pointer);
    }
    DoInsertCell(target_page, primary_key, cell);
  }
}

PageIndex TableManager::SearchPage(const PageIndex& current_page,
                                   const PrimaryKey& primary_key) {
  if (IsLeaf(current_page)) {
    return current_page;
  }

  CellKeyRange key_range = page_list_[current_page].GetCellKeyRange();

  if (primary_key < key_range.first) {
    return SearchPage(page_list_[current_page].GetLeftMostPagePointer(),
                      primary_key);
  } else if (primary_key > key_range.first && primary_key < key_range.second) {
    return current_page;
  } else if (primary_key > key_range.second) {
    return SearchPage(page_list_[current_page].GetRightMostPagePointer(),
                      primary_key);
  }
}

PageIndex TableManager::CreatePage(const PageType& page_type) {
  page_list_.emplace_back(table_file_, page_list_.size() * page_size);
  page_list_.back().SetPageType(page_type);
  page_list_.back().Clear();
  page_list_.back().UpdateInfo();
  return (page_list_.size() - 1);
}

void TableManager::LoadPage(void) {
  utils::FileSize size = table_file_->GetFileSize();

  // TODO: change assert to exception
  assert(size % page_size == 0);

  page_num_ = size / page_size;
  for (auto i = 0; i < page_num_; i++) {
    page_list_.emplace_back(table_file_, i * page_size);
    page_list_.back().ParseInfo();
  }
}

PageIndex TableManager::SplitInteriorPage(
    const PageIndex& target_page, const CellPivot& cell_pivot,
    const PrimaryKey& primary_key, const PageCell& cell,
    std::shared_ptr<PageIndex> right_most_pointer) {
  assert(page_list_[target_page].GetPageType() == TableInteriorCell);

  CellIndex copy_index(0);
  CellIndex insert_index(0);
  CellIndex delete_index(0);
  PageIndex new_page(CreatePage(TableInteriorCell));
  auto iter_target = page_list_.begin() + target_page;
  uint8_t target_cell_num(iter_target->GetCellNum());
  CellKeyRange target_key_range(iter_target->GetCellKeyRange());

  // up key is in the target
  if (primary_key != cell_pivot.second) {
    delete_index = cell_pivot.first;
    copy_index = delete_index + 1;
  } else {
    delete_index = cell_pivot.first;
    copy_index = delete_index;
  }

  // update pointers
  if (primary_key > target_key_range.second) {
    // lower level right most split to target
    SetRightMostPointer(new_page, *right_most_pointer);

    // use up key's left pointer as target right pointer
    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    insert_index = new_page;

  } else if (primary_key < target_key_range.first) {
    // lower level left most split to target
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    // first key's left pointer
    iter_target->SetCellLeftPointer(0, *right_most_pointer);

    // use up key's left pointer as target right pointer
    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    insert_index = target_page;

  } else if (GetCellKey(target_page, delete_index - 1) < primary_key &&
             primary_key < cell_pivot.second) {
    // primary key is just left to pivot
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    SetRightMostPointer(target_page, *right_most_pointer);

    insert_index = target_page;

  } else if (primary_key > cell_pivot.second &&
             primary_key < GetCellKey(target_page, delete_index + 1)) {
    // primary key is just right to pivot
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    iter_target->SetCellLeftPointer(delete_index + 1, *right_most_pointer);

    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    insert_index = new_page;

  } else if (primary_key == cell_pivot.second) {
    // primary key is pivot
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    iter_target->SetCellLeftPointer(delete_index, *right_most_pointer);

    insert_index = new_page;
  } else {
    std::cerr << "not support" << std::endl;
  }

  // copy cells into new page
  for (auto i = copy_index; i < target_cell_num; i++) {
    DoInsertCell(new_page, iter_target->GetCellKey(i), iter_target->GetCell(i));
  }

  // always this index because of vector
  for (auto j = delete_index; j < target_cell_num; j++) {
    iter_target->DeleteCell(delete_index);
  }

  // update changes
  iter_target->UpdateInfo();

  // insert this cell
  DoInsertCell(insert_index, primary_key, cell);

  // update parent
  UpdateParent(target_page);
  UpdateParent(new_page);

  return new_page;
}

PageIndex TableManager::SplitLeafPage(const PageIndex& target_page,
                                      const CellIndex& cell_index,
                                      const PrimaryKey& primary_key,
                                      const PageCell& cell) {
  assert(page_list_[target_page].GetPageType() == TableLeafCell);

  PageIndex new_page(CreatePage(TableLeafCell));
  auto iter_target = page_list_.begin() + target_page;
  uint8_t target_cell_num(iter_target->GetCellNum());

  // move cells into new page (fix index because of vector)
  for (auto i = cell_index; i < target_cell_num; i++) {
    DoInsertCell(new_page, iter_target->GetCellKey(cell_index),
                 iter_target->GetCell(cell_index));
    iter_target->DeleteCell(cell_index);
  }

  iter_target->SetPageRightMostPointer(new_page);

  // update changes
  iter_target->UpdateInfo();

  // insert this cell
  DoInsertCell(new_page, primary_key, cell);

  return new_page;
}

PageIndex TableManager::GetParent(const PageIndex& page_index) {
  return (page_list_[page_index].GetParent());
}

bool TableManager::WillOverflow(const PageIndex& page_index) const {
  return (page_list_[page_index].GetCellNum() + 1 > fanout_ - 1);
}

void TableManager::UpdateFanout(const PageIndex& page_index) {
  if (!IsRoot(page_index) || !IsLeaf(page_index)) {
    return;
  }
  fanout_ = static_cast<uint8_t>(page_list_[page_index].GetCellNum() + 1);
}

void TableManager::DoInsertCell(const PageIndex& page_index,
                                const PrimaryKey& primary_key,
                                const PageCell& cell) {
  page_list_[page_index].InsertCell(primary_key, cell);
}

CellPivot TableManager::GetCellPivot(const PageIndex& page_index,
                                     const CellKey& cell_key) {
  CellPivot pivot;

  std::set<CellKey> key_set(page_list_[page_index].GetCellKeySet());
  key_set.insert(cell_key);

  // range constructor
  std::vector<CellKey> key_list(key_set.begin(), key_set.end());
  pivot.second = key_list[key_list.size() / 2];

  key_set.erase(cell_key);
  // first element that is not less than pivot
  pivot.first =
      std::distance(key_set.begin(), key_set.lower_bound(pivot.second));

  return pivot;
}

void TableManager::UpdateParent(const PageIndex& page_index) {
  auto iter = page_list_.begin() + page_index;
  for (auto i = 0; i < iter->GetCellNum(); ++i) {
    SetParent(iter->GetCellLeftPointer(i), page_index);
  }
  SetParent(iter->GetRightMostPagePointer(), page_index);
}

void TableManager::LoadParent(const PageIndex& page_index) {
  if (IsLeaf(page_index)) {
    return;
  }

  auto iter = page_list_.begin() + page_index;
  for (auto i = 0; i < iter->GetCellNum(); ++i) {
    SetParent(iter->GetCellLeftPointer(i), page_index);
    LoadParent(iter->GetCellLeftPointer(i));
  }
  SetParent(iter->GetRightMostPagePointer(), page_index);
  LoadParent(iter->GetRightMostPagePointer());
}

}  // namespace internal
