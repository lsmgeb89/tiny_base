#include <bitset>
#include <cassert>
#include <cstring>
#include <iostream>

#include "endian_util.h"
#include "page_manager.h"
#include "page_format.h"

namespace internal {

PageManager::PageManager(utils::FileHandle& table_file,
                         const utils::FilePosition& page_base)
    : table_file_(table_file),
      page_base_(page_base),
      page_type_(InvalidCell),
      cell_num_(0),
      cell_content_offset_(page_size),
      right_most_pointer_(0),
      parent_(0) {}

void PageManager::ParseInfo(void) {
  std::vector<uint8_t> data_in;
  data_in.resize(table_header_length);

  // TODO: exception handling
  table_file_->Read(page_base_, reinterpret_cast<char*>(data_in.data()),
                    table_header_length);

  // page header
  page_type_ = static_cast<PageType>(data_in[page_type_offset]);
  cell_num_ = data_in[cell_num_offset];
  std::memcpy(&cell_content_offset_,
              data_in.data() + cell_content_offset_offset,
              cell_content_offset_length);
  cell_content_offset_ =
      utils::SwapEndian<decltype(cell_content_offset_)>(cell_content_offset_);
  std::memcpy(&right_most_pointer_, data_in.data() + right_most_pointer_offset,
              right_most_pointer_length);
  right_most_pointer_ =
      utils::SwapEndian<decltype(right_most_pointer_)>(right_most_pointer_);

  // cell point array
  if (cell_num_) {
    cell_pointer_array_.resize(cell_num_);
    table_file_->Read(page_base_ + table_header_length,
                      reinterpret_cast<char*>(cell_pointer_array_.data()),
                      cell_num_ * cell_pointer_length);
    utils::SwapEndianInPlace<decltype(cell_pointer_array_)::value_type>(
        cell_pointer_array_);
  }

  // cell key
  for (auto i = 0; i < cell_num_; i++) {
    key_set_.insert(GetCellKey(i));
  }
}

CellKeyRange PageManager::GetCellKeyRange(void) {
  CellKey min_key = GetCellKey(0);
  CellKey max_key = GetCellKey(static_cast<uint8_t>(cell_num_ - 1));
  return std::make_pair(min_key, max_key);
}

CellKey PageManager::GetCellKey(const CellIndex& cell_index) const {
  CellKey key;
  uint8_t offset(0);
  uint8_t length(0);

  if (TableInteriorCell == page_type_) {
    offset = table_interior_key_offset;
    length = table_interior_key_length;
  } else if (TableLeafCell == page_type_) {
    offset = table_leaf_rowid_offset;
    length = table_leaf_rowid_length;
  }

  table_file_->Read(page_base_ + cell_pointer_array_[cell_index] + offset,
                    reinterpret_cast<char*>(&key), length);

  return utils::SwapEndian<decltype(key)>(key);
}

PageCell PageManager::GetCell(const CellIndex& cell_index) const {
  PageCell cell;
  uint16_t cell_size;

  if (TableLeafCell == page_type_) {
    table_file_->Read(page_base_ + cell_pointer_array_[cell_index],
                      reinterpret_cast<char*>(&cell_size),
                      table_leaf_payload_length_length);
    cell_size = utils::SwapEndian<decltype(cell_size)>(cell_size);
    cell_size += table_leaf_payload_offset;
  } else if (TableInteriorCell == page_type_) {
    cell_size = table_interior_cell_length;
  }

  cell.resize(cell_size);

  table_file_->Read(page_base_ + cell_pointer_array_[cell_index], cell.data(),
                    cell_size);

  return cell;
}

PagePointer PageManager::GetCellLeftPointer(const CellIndex& cell_index) const {
  PagePointer left_pointer;

  assert(TableInteriorCell == page_type_);

  table_file_->Read(page_base_ + cell_pointer_array_[cell_index],
                    reinterpret_cast<char*>(&left_pointer),
                    table_interior_left_pointer_length);

  left_pointer = utils::SwapEndian<decltype(left_pointer)>(left_pointer);

  return left_pointer;
}

PageIndex PageManager::GetLeftMostPagePointer(void) {
  PageIndex page_pointer;

  if (TableLeafCell == page_type_) {
    // TODO: throw exception
  }

  table_file_->Read(page_base_ + cell_pointer_array_[0],
                    reinterpret_cast<char*>(&page_pointer),
                    table_interior_left_pointer_length);
  return utils::SwapEndian<decltype(page_pointer)>(page_pointer);
}

bool PageManager::HasSpace(const utils::FileOffset& cell_size) const {
  utils::FilePosition cell_pointer_array_end =
      table_header_length + cell_num_ * cell_pointer_length;

  utils::FilePosition free_space =
      cell_content_offset_ - cell_pointer_array_end;

  return (free_space >= cell_size + cell_pointer_length);
}

void PageManager::InsertCell(const CellKey& primary_key, const PageCell& cell) {
  cell_content_offset_ -= cell.size();

  key_set_.insert(primary_key);

  auto cell_index = std::distance(key_set_.begin(), key_set_.find(primary_key));

  // TODO: recover cell_content_offset_ while error
  table_file_->Write(page_base_ + cell_content_offset_, cell.data(),
                     cell.size());
  ++cell_num_;

  // TODO: check the cell_index is in the range of array (assert)
  cell_pointer_array_.insert(cell_pointer_array_.begin() + cell_index,
                             cell_content_offset_);
  UpdateInfo();
}

void PageManager::DeleteCell(const CellIndex& cell_index) {
  if (cell_index >= cell_num_) {
    return;
  }

  // delete cell pointer
  cell_pointer_array_.erase(cell_pointer_array_.begin() + cell_index);

  // remove key in the set
  std::vector<CellKey> key_list(key_set_.begin(), key_set_.end());
  key_list.erase(key_list.begin() + cell_index);
  key_set_.clear();
  std::copy(key_list.begin(), key_list.end(),
            std::inserter(key_set_, key_set_.begin()));

  // decrease number
  --cell_num_;
}

void PageManager::UpdateInfo(void) {
  auto value(0);
  std::vector<uint8_t> data_out;
  auto length(table_header_length + cell_num_ * cell_pointer_length);
  data_out.resize(length);

  // start filling data
  data_out[page_type_offset] = page_type_;
  data_out[cell_num_offset] = cell_num_;

  value =
      utils::SwapEndian<decltype(cell_content_offset_)>(cell_content_offset_);
  std::memcpy(data_out.data() + cell_content_offset_offset, &value,
              cell_content_offset_length);

  value = utils::SwapEndian<decltype(right_most_pointer_)>(right_most_pointer_);
  std::memcpy(data_out.data() + right_most_pointer_offset, &value,
              right_most_pointer_length);

  // TODO: check copy will work or not because data type is not the same
  if (cell_num_) {
    std::memcpy(data_out.data() + cell_pointer_array_offset,
                utils::SwapEndian<decltype(cell_pointer_array_)::value_type>(
                    cell_pointer_array_).data(),
                cell_num_ * cell_pointer_length);
  }

  table_file_->Write(page_base_, reinterpret_cast<char*>(data_out.data()),
                     length);
}

void PageManager::Clear(void) {
  std::vector<uint8_t> data_out(page_size, 0);
  table_file_->Write(page_base_, reinterpret_cast<char*>(data_out.data()),
                     page_size);
}

void PageManager::SetCellLeftPointer(const CellIndex& cell_index,
                                     const PagePointer& left_pointer) {
  PagePointer data_out(utils::SwapEndian<PagePointer>(left_pointer));
  table_file_->Write(page_base_ + cell_pointer_array_[cell_index],
                     reinterpret_cast<char*>(&data_out),
                     table_interior_left_pointer_length);
}

void PageManager::Reset(void) {
  cell_num_ = 0;
  cell_content_offset_ = page_size;
  cell_pointer_array_.clear();
  key_set_.clear();
}

void PageManager::Reorder(void) {
  std::vector<std::pair<CellKey, PageCell>> cell_list;

  // Read back cell
  for (auto i = 0; i < cell_num_; i++) {
    cell_list.push_back(std::make_pair(GetCellKey(i), GetCell(i)));
  }

  // Fill zero
  Clear();

  // Reset for insert
  Reset();

  for (auto cell_pair : cell_list) {
    InsertCell(cell_pair.first, cell_pair.second);
  }

  if (!cell_list.size()) {
    UpdateInfo();
  }
}

bool PageManager::FindCell(const CellKey& key, PageCell& cell) const {
  auto res = key_set_.find(key);
  bool ret = (res != key_set_.end());
  if (ret) {
    cell = GetCell(std::distance(key_set_.begin(), res));
  }
  return ret;
}

void PageManager::AppendAllCells(std::vector<PageCell>& tuples) const {
  for (auto i = 0; i < cell_num_; i++) {
    tuples.push_back(GetCell(i));
  }
}

bool PageManager::UpdateCell(const CellKey& key, const PageCell& cell) {
  CellIndex cell_index(0);
  auto res = key_set_.find(key);
  bool ret = (res != key_set_.end());
  if (ret) {
    cell_index = std::distance(key_set_.begin(), res);
    table_file_->Write(page_base_ + cell_pointer_array_[cell_index],
                       cell.data(), cell.size());
  }

  return ret;
}

CellIndex PageManager::GetCellIndex(const CellKey& cell_key) const {
  return std::distance(key_set_.begin(), key_set_.find(cell_key));
}

bool PageManager::IsKeyDuplicate(const CellKey& key) const {
  return (key_set_.find(key) != key_set_.end());
}

}  // namespace internal
