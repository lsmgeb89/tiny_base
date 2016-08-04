#ifndef TINY_BASE_CELL_H_
#define TINY_BASE_CELL_H_

#include "page_format.h"
#include "page_manager.h"
#include "sql_command.h"

namespace internal {

static PageCell GetValue(const PageCell& cell, const std::ptrdiff_t& index) {
  PageCell value;
  auto column_num(cell.at(table_leaf_payload_num_of_columns_offset));
  auto offset(table_leaf_payload_type_codes_offset + column_num);

  // calc offset
  for (auto i = 0; i < index; i++) {
    auto type_code = cell.at(table_leaf_payload_type_codes_offset + i);
    auto size = sql::TypeCodeToSize(type_code);
    offset += size;
  }

  // get value
  auto value_type(cell.at(table_leaf_payload_type_codes_offset + index));
  auto value_size(sql::TypeCodeToSize(value_type));
  value.resize(value_size);
  std::copy(cell.begin() + offset, cell.begin() + offset + value_size,
            value.begin());

  // swap endian
  std::reverse(value.begin(), value.end());

  return value;
}

static void GetValues(const PageCell& cell,
                      const std::vector<std::ptrdiff_t>& indexes,
                      std::vector<PageCell>& values) {
  PageCell value;
  auto column_num(cell.at(table_leaf_payload_num_of_columns_offset));
  auto offset(table_leaf_payload_type_codes_offset + column_num);
  auto value_type(0);
  auto value_size(0);

  // loop through cell
  for (auto i = 0, j = 0; i < column_num, j < column_num; i++) {
    value_type = cell.at(table_leaf_payload_type_codes_offset + i);
    value_size = sql::TypeCodeToSize(value_type);
    // accumulate offset
    if (i != indexes.at(j)) {
      offset += value_size;
      continue;
    }
    // get value
    value.clear();
    value.resize(value_size);
    std::copy(cell.begin() + offset, cell.begin() + offset + value_size,
              value.begin());
    // swap endian
    std::reverse(value.begin(), value.end());
    values.push_back(value);

    offset += value_size;
    j++;
  }
}

static bool UpdateValue(PageCell& cell, const std::ptrdiff_t& index,
                        const sql::TypeCode& type_code,
                        const sql::Value& value) {
  PageCell value_bytes;
  sql::TypeCode old_type_code(
      cell.at(table_leaf_payload_type_codes_offset + index));
  auto old_type_size(sql::TypeCodeToSize(old_type_code));
  auto new_type_size(sql::TypeCodeToSize(type_code));

  if (old_type_size < new_type_size) {
    return false;
  }

  auto column_num(cell.at(table_leaf_payload_num_of_columns_offset));
  auto offset(table_leaf_payload_type_codes_offset + column_num);

  // calc offset
  for (auto i = 0; i < index; i++) {
    auto type_code = cell.at(table_leaf_payload_type_codes_offset + i);
    auto size = sql::TypeCodeToSize(type_code);
    offset += size;
  }

  // modify type code
  cell.at(table_leaf_payload_type_codes_offset + index) = type_code;

  // prepare value
  sql::ValueToBytes(type_code, value, value_bytes);

  // swap endian
  std::reverse(value_bytes.begin(), value_bytes.end());

  if (old_type_size == new_type_size) {
    // directly modify value bytes
    std::memcpy(cell.data() + offset, value_bytes.data(), value_bytes.size());
  } else if (old_type_size > new_type_size) {
    // store remaining part to a temp buffer
    PageCell temp_cell;
    temp_cell.resize(cell.size() - offset - old_type_size);
    std::copy(cell.begin() + offset + old_type_size, cell.end(),
              temp_cell.begin());

    // copy new value (new_type_size == value_bytes.size())
    std::copy(value_bytes.begin(), value_bytes.end(), cell.begin() + offset);

    // copy back remaining part
    std::copy(temp_cell.begin(), temp_cell.end(),
              cell.begin() + offset + new_type_size);

    // shrink
    cell.resize(cell.size() - (old_type_size - new_type_size));
  }

  return true;
}

static sql::TypeCode GetTypeCode(const PageCell& cell,
                                 const std::ptrdiff_t& index) {
  return cell.at(table_leaf_payload_type_codes_offset + index);
}

static void GetTypeCodes(const PageCell& cell,
                         const std::vector<std::ptrdiff_t>& indexes,
                         std::vector<sql::TypeCode>& type_codes) {
  for (auto index : indexes) {
    type_codes.push_back(GetTypeCode(cell, index));
  }
}

}  // namespace internal

#endif  // TINY_BASE_CELL_H_
