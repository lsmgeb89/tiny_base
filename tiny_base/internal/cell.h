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
