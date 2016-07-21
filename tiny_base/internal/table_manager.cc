#include <algorithm>
#include <cstring>

#include "endian_util.h"
#include "page_format.h"
#include "table_manager.h"

namespace internal {

TableManager::TableManager(const fs::path& file_path)
    : file_path_(file_path),
      root_page_(0),
      page_num_(0),
      fanout_(0),
      table_file_(std::make_shared<utils::FileUtil>(file_path_)) {}

TableManager::~TableManager(void) {}

void TableManager::Load(const TableSchema& schema) {
  table_schema_ = schema;
  LoadPage();
}

void TableManager::CreateTable(const sql::CreateTableCommand& command) {
  if (fs::exists(file_path_)) {
    // TODO: throw error
  }

  // save schema
  table_schema_ = command;

  fs::create_directories(file_path_.parent_path());

  table_file_->CreateFile();

  CreatePage(TableLeafCell);
}

void TableManager::InsertInto(const sql::InsertIntoCommand& command) {
  // key
  InsertRecord(GetPrimaryKey(command), PrepareRecord(command));
}

PrimaryKey TableManager::GetPrimaryKey(const sql::InsertIntoCommand& command) {
  PrimaryKey primary_key =
      std::experimental::any_cast<int32_t>(command.value_list[0]);
  return primary_key;
}

PageRecord TableManager::PrepareRecord(const sql::InsertIntoCommand& command) {
  std::size_t offset(0);
  std::size_t type_size(0);
  PageRecord tiny_tables_record;

  if (table_schema_.column_list.size() != command.value_list.size()) {
    // TODO: error handling
  }

  for (auto i = 0; i < table_schema_.column_list.size(); i++) {
    if (table_schema_.column_list[i].type != sql::Text) {
      type_size = sql::DataTypeSize[table_schema_.column_list[i].type];
      tiny_tables_record.resize(tiny_tables_record.size() + type_size);
    }

    switch (table_schema_.column_list[i].type) {
      case sql::OneByteNull:
      case sql::TinyInt:
        tiny_tables_record[offset] =
            std::experimental::any_cast<int8_t>(command.value_list[i]);
        break;
      case sql::TwoByteNull:
      case sql::SmallInt: {
        int16_t value_16 =
            std::experimental::any_cast<int16_t>(command.value_list[i]);
        value_16 = utils::SwapEndian<decltype(value_16)>(value_16);
        std::memcpy(tiny_tables_record.data() + offset, &value_16, type_size);
      } break;
      case sql::FourByteNull:
      case sql::Int: {
        int32_t value_32 =
            std::experimental::any_cast<int32_t>(command.value_list[i]);
        value_32 = utils::SwapEndian<decltype(value_32)>(value_32);
        std::memcpy(tiny_tables_record.data() + offset, &value_32, type_size);
      } break;
      case sql::EightByteNull:
      case sql::BigInt: {
        int64_t value_64 =
            std::experimental::any_cast<int64_t>(command.value_list[i]);
        value_64 = utils::SwapEndian<decltype(value_64)>(value_64);
        std::memcpy(tiny_tables_record.data() + offset, &value_64, type_size);
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
        tiny_tables_record.resize(tiny_tables_record.size() + type_size);
        std::reverse(value_str.begin(), value_str.end());
        std::copy(value_str.begin(), value_str.end(),
                  tiny_tables_record.data() + offset);
      } break;
      default:
        break;
    }

    offset += type_size;
  }
  return tiny_tables_record;
}

void TableManager::InsertRecord(const PrimaryKey& primary_key,
                                const PageRecord& record) {
  PageIndex target_page = SearchPage(root_page_, primary_key);

  if (!fanout_) {
    if (HasSpace(target_page, record.size())) {
      InsertRecord_(target_page, primary_key, record);
    } else {
      UpdateFanout(target_page);
      // split
    }
  } else if (IsRoot(target_page)) {
    // split
  } else if (IsOverflow(target_page) || !HasSpace(target_page, record.size())) {
    // split
  } else {
    InsertRecord_(target_page, primary_key, record);
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

void TableManager::CreatePage(const PageType& page_type) {
  page_list_.emplace_back(table_file_, page_list_.size() * page_size);
  page_list_.back().SetPageType(TableLeafCell);
  page_list_.back().UpdateInfo();
}

void TableManager::LoadPage(void) {
  utils::FileSize size = table_file_->GetFileSize();
  // TODO: all should be multiple page_size
  if (size % page_size) {
    return;
  }
  page_num_ = size / page_size;
  for (auto i = 0; i < page_num_; i++) {
    page_list_.emplace_back(table_file_, i * page_size);
    page_list_.back().ParseInfo();
  }
}

bool TableManager::IsOverflow(const PageIndex& page_index) const {
  return (page_list_[page_index].GetCellNum() + 1 <= fanout_);
}

void TableManager::UpdateFanout(const PageIndex& page_index) {
  if (!IsRoot(page_index) || !IsLeaf(page_index)) {
    return;
  }
  fanout_ = static_cast<uint8_t>(page_list_[page_index].GetCellNum() + 1);
}

void TableManager::InsertRecord_(const PageIndex& page_index,
                                 const PrimaryKey& primary_key,
                                 const PageRecord& record) {
  PageManager& page(page_list_[page_index]);
  auto cell_num = page.GetCellNum();
  if (!cell_num) {
    page.InsertRecord(0, record);
  } else {
    CellKeyRange key_range = page.GetCellKeyRange();
    if (primary_key < key_range.first) {
      page.InsertRecord(0, record);
    } else if (primary_key > key_range.second) {
      page.InsertRecord(cell_num, record);
    } else {
      for (auto i = 1; i < cell_num; i++) {
        if (primary_key < page.GetCellKey(i)) {
          page.InsertRecord(i, record);
          break;
        }
      }
    }
  }
}

}  // namespace internal
