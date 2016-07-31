#include <algorithm>
#include <cstring>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "cell.h"
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

const std::string TableManager::SelectFrom(
    const sql::SelectFromCommand& command) {
  std::vector<PageCell> tuples;

  PullTuple(command, tuples);

  return FilterTuple(command, tuples);
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

  // copy rowid (leave space for number of payload)
  table_leaf_cell.resize(table_leaf_cell.size() + table_leaf_rowid_offset +
                         table_leaf_rowid_length);
  std::memcpy(table_leaf_cell.data() + offset, &primary_key,
              table_leaf_rowid_length);
  offset += table_leaf_rowid_length;

  // payload header
  uint8_t number_of_columns(static_cast<uint8_t>(command.value_list.size()));
  table_leaf_cell.resize(table_leaf_cell.size() +
                         table_leaf_payload_num_of_columns_length);
  table_leaf_cell.at(offset) = number_of_columns;
  offset += table_leaf_payload_num_of_columns_length;

  uint8_t type_code(0);
  table_leaf_cell.resize(table_leaf_cell.size() +
                         table_leaf_payload_type_code_length *
                             number_of_columns);
  for (auto i = 0; i < number_of_columns; i++) {
    type_code = table_schema_.column_list.at(i).type;
    table_leaf_cell.at(offset) = type_code;
    offset += table_leaf_payload_type_code_length;
  }

  // payload body
  for (auto i = 0; i < number_of_columns; i++) {
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
      case sql::Real: {
        float value_float =
            sql::expr::any_cast<float>(command.value_list.at(i));
        value_float = utils::SwapEndian<decltype(value_float)>(value_float);
        std::memcpy(table_leaf_cell.data() + offset, &value_float, type_size);
      } break;
      case sql::Double: {
        double value_double =
            sql::expr::any_cast<double>(command.value_list.at(i));
        value_double = utils::SwapEndian<decltype(value_double)>(value_double);
        std::memcpy(table_leaf_cell.data() + offset, &value_double, type_size);
      } break;
      case sql::DateTime: {
        uint64_t value_date_time =
            sql::expr::any_cast<uint64_t>(command.value_list.at(i));
        value_date_time =
            utils::SwapEndian<decltype(value_date_time)>(value_date_time);
        std::memcpy(table_leaf_cell.data() + offset, &value_date_time,
                    type_size);
      } break;
      case sql::Date: {
        uint64_t value_date =
            sql::expr::any_cast<uint64_t>(command.value_list.at(i));
        value_date = utils::SwapEndian<decltype(value_date)>(value_date);
        std::memcpy(table_leaf_cell.data() + offset, &value_date, type_size);
      } break;
      case sql::Text: {
        std::string value_str =
            std::experimental::any_cast<std::string>(command.value_list[i]);
        type_size = value_str.size();
        table_leaf_cell.resize(table_leaf_cell.size() + type_size);
        std::reverse(value_str.begin(), value_str.end());
        std::copy(value_str.begin(), value_str.end(),
                  table_leaf_cell.data() + offset);

        // update real type for string
        table_leaf_cell.at(table_leaf_payload_type_codes_offset + i) =
            sql::Text + type_size;
      } break;
      default:
        break;
    }

    offset += type_size;
  }

  // payload size
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
  if (WillOverflow(target_page) || !HasSpace(target_page, cell.size())) {
    PageIndex parent_page(0);
    PageIndex left_child_page(0);
    std::shared_ptr<PageIndex> right_child_page(std::make_shared<PageIndex>(0));
    CellPivot cell_pivot(GetCellPivot(target_page, primary_key));

    // right split
    PageIndex new_page(0);
    if (IsLeaf(target_page)) {
      new_page = SplitLeafPage(target_page, cell_pivot, primary_key, cell);
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
    if (!IsLeaf(target_page)) {
      const CellIndex bound(GetLowerBound(target_page, primary_key));
      assert(right_most_pointer);
      if (bound == GetCellNum(target_page)) {
        SetRightMostPointer(target_page, *right_most_pointer);
      } else {
        SetCellLeftPointer(target_page, bound, *right_most_pointer);
      }
    } else {
      if (right_most_pointer) {
        SetRightMostPointer(target_page, *right_most_pointer);
      }
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
    return GetCellLeftPointer(current_page,
                              GetLowerBound(current_page, primary_key));
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
  CellIndex insert_index(std::numeric_limits<CellIndex>::max());
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

  } else if (target_key_range.first < primary_key &&
             primary_key < GetCellKey(target_page, delete_index - 1)) {
    // primary key is between minimum and pivot
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    iter_target->SetCellLeftPointer(GetLowerBound(target_page, primary_key),
                                    *right_most_pointer);

    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    insert_index = target_page;

  } else if (primary_key > GetCellKey(target_page, delete_index + 1) &&
             primary_key < target_key_range.second) {
    // primary key is between pivot and maximum
    SetRightMostPointer(new_page, GetRightMostPointer(target_page));

    iter_target->SetCellLeftPointer(GetLowerBound(target_page, primary_key),
                                    *right_most_pointer);

    SetRightMostPointer(target_page,
                        iter_target->GetCellLeftPointer(delete_index));

    insert_index = new_page;

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

  // recorder
  iter_target->Reorder();

  // insert this cell
  if (insert_index != std::numeric_limits<CellIndex>::max()) {
    DoInsertCell(insert_index, primary_key, cell);
  }

  // update parent
  UpdateParent(target_page);
  UpdateParent(new_page);

  return new_page;
}

PageIndex TableManager::SplitLeafPage(const PageIndex& target_page,
                                      const CellPivot& cell_pivot,
                                      const PrimaryKey& primary_key,
                                      const PageCell& cell) {
  assert(page_list_[target_page].GetPageType() == TableLeafCell);

  PageIndex new_page(CreatePage(TableLeafCell));
  CellIndex insert_index(primary_key >= cell_pivot.second ? new_page
                                                          : target_page);
  auto iter_target = page_list_.begin() + target_page;
  auto iter_new = page_list_.begin() + new_page;
  uint8_t target_cell_num(iter_target->GetCellNum());

  // move cells into new page (fixed index because of vector)
  for (auto i = cell_pivot.first; i < target_cell_num; i++) {
    DoInsertCell(new_page, iter_target->GetCellKey(cell_pivot.first),
                 iter_target->GetCell(cell_pivot.first));
    iter_target->DeleteCell(cell_pivot.first);
  }

  // right most pointer
  SetRightMostPointer(new_page, GetRightMostPointer(target_page));
  SetRightMostPointer(target_page, new_page);

  // update changes
  iter_target->UpdateInfo();
  iter_new->UpdateInfo();

  // recorder
  iter_target->Reorder();

  // insert this cell
  DoInsertCell(insert_index, primary_key, cell);

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

void TableManager::PullTupleWithPrimary(const sql::SelectFromCommand& command,
                                        std::vector<PageCell>& tuples) {
  PageRange range;
  int32_t condition_value = sql::expr::any_cast<int32_t>(command.where->value);

  PageIndex target_page(SearchPage(root_page_, condition_value));
  PageIndex min_page(
      SearchPage(root_page_, std::numeric_limits<PrimaryKey>::min()));
  PageIndex max_page(
      SearchPage(root_page_, std::numeric_limits<PrimaryKey>::max()));

  switch (command.where->condition_operator) {
    case sql::Equal:
      range = std::make_pair(target_page, target_page);
      break;
    case sql::Unequal:
      range = std::make_pair(min_page, max_page);
      break;
    case sql::Larger:
      range = std::make_pair(target_page, max_page);
      break;
    case sql::Smaller:
      range = std::make_pair(min_page, target_page);
      break;
    case sql::NotLarger:
      range = std::make_pair(min_page, target_page);
      break;
    case sql::NotSmaller:
      range = std::make_pair(target_page, max_page);
      break;
    default:
      break;
  }

  if (range.first == range.second) {
    page_list_.at(range.first).AppendAllCells(tuples);
  } else {
    PageIndex iter = range.first;
    PageIndex iter_end = GetRightMostPointer(range.second);

    do {
      page_list_.at(iter).AppendAllCells(tuples);
      iter = GetRightMostPointer(iter);
    } while (iter != iter_end);
  }
}

void TableManager::PullTuple(const sql::SelectFromCommand& command,
                             std::vector<PageCell>& tuples) {
  // with primary key condition
  if (command.where && IsPrimaryKey(command.where->column_name)) {
    PullTupleWithPrimary(command, tuples);
  } else {
    // iterate through leaf page
    PageIndex iter(
        SearchPage(root_page_, std::numeric_limits<PrimaryKey>::min()));

    do {
      page_list_.at(iter).AppendAllCells(tuples);
      iter = page_list_.at(iter).GetRightMostPagePointer();
      // TODO: start page index from 1 or find a way to identify zero leaf page
      // in the middle
    } while (iter);
  }
}

const std::string TableManager::FilterTuple(
    const sql::SelectFromCommand& command, std::vector<PageCell>& tuples) {
  // gather type info
  std::vector<std::ptrdiff_t> column_indexes;
  std::vector<sql::DataType> column_types;
  std::vector<std::size_t> column_max_length;
  for (auto name : command.column_name) {
    auto index(GetColumnIndex(name));
    column_indexes.push_back(index);
    column_types.push_back(table_schema_.column_list.at(index).type);
    column_max_length.push_back(name.size());
  }

  std::ptrdiff_t cond_var_type_index(0);
  sql::DataType cond_var_type(sql::Invalid);
  if (command.where) {
    cond_var_type_index = GetColumnIndex(command.where->column_name);
    cond_var_type = table_schema_.column_list.at(cond_var_type_index).type;
  }

  // core loop
  bool expr_res;
  PageCell value;
  sql::Value lhs;
  std::vector<std::vector<std::string>> out_str;
  auto iter = tuples.begin();
  while (iter != tuples.end()) {
    // apply condition
    if (command.where) {
      value = GetValue(*iter, cond_var_type_index);
      lhs = sql::BytesToValue(cond_var_type, value);
      expr_res = sql::CompareValue(lhs, command.where->value, cond_var_type,
                                   command.where->condition_operator);

      if (!expr_res) {
        iter = tuples.erase(iter);
        continue;
      }
    }

    // select columns
    std::vector<std::string> tuple_str;
    for (auto i = 0; i < column_indexes.size(); i++) {
      value = GetValue(*iter, column_indexes.at(i));
      std::string value_str = sql::BytesToString(column_types.at(i), value);
      tuple_str.push_back(value_str);
      if (column_max_length.at(i) < value_str.size()) {
        column_max_length.at(i) = value_str.size();
      }
    }
    out_str.push_back(tuple_str);

    // next line
    iter++;
  }

  // final output
  std::stringstream out_stream;

  if (out_str.empty()) {
    out_stream << "Empty set";
  } else {
    // form delimited line
    std::string delimit_line("+");
    for (auto i = 0; i < column_max_length.size(); i++) {
      delimit_line.append(column_max_length.at(i) + 2, '-');
      delimit_line.append("+");
    }
    delimit_line.append("\n");

    // print header
    out_stream << delimit_line;
    for (auto i = 0; i < column_max_length.size(); i++) {
      out_stream << "| " << command.column_name.at(i);
      out_stream << std::setw(column_max_length.at(i) -
                              command.column_name.at(i).size() + 1) << " ";
    }
    out_stream << "|\n" << delimit_line;

    // print body
    for (auto tuple_str : out_str) {
      // print each line
      for (auto i = 0; i < column_max_length.size(); i++) {
        out_stream << "| " << tuple_str.at(i);
        out_stream << std::setw(column_max_length.at(i) -
                                tuple_str.at(i).size() + 1) << " ";
      }
      out_stream << "|\n";
    }

    // print tail
    out_stream << delimit_line;
    out_stream << out_str.size() << " rows in set\n";
  }

  return out_stream.str();
}

std::ptrdiff_t TableManager::GetColumnIndex(const std::string& column_name) {
  auto res = std::find_if(table_schema_.column_list.begin(),
                          table_schema_.column_list.end(),
                          [column_name](const sql::CreateTableColumn& arg) {
                            return arg.column_name == column_name;
                          });
  return std::distance(table_schema_.column_list.begin(), res);
}

}  // namespace internal
