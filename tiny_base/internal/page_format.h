#ifndef TINY_BASE_PAGE_FORMAT_H_
#define TINY_BASE_PAGE_FORMAT_H_

#include <cstdint>
#include "file_util.h"

namespace internal {

constexpr uint16_t page_size = 512;

/* Table Header Format */
constexpr uint8_t page_type_offset = 0x00;
constexpr uint8_t page_type_length = 1;

constexpr uint8_t cell_num_offset = (page_type_offset + page_type_length);
constexpr uint8_t cell_num_length = 1;

constexpr uint8_t cell_content_offset_offset =
    (cell_num_offset + cell_num_length);
constexpr uint8_t cell_content_offset_length = 2;

constexpr uint8_t right_most_pointer_offset =
    (cell_content_offset_offset + cell_content_offset_length);
constexpr uint8_t right_most_pointer_length = 4;

constexpr uint8_t table_header_length =
    (right_most_pointer_offset + right_most_pointer_length);

/* Cell Pointer Format */
constexpr uint8_t cell_pointer_array_offset = table_header_length;
constexpr uint8_t cell_pointer_length = 2;

/* Table B-Tree Leaf Cell */
constexpr uint8_t table_leaf_payload_length_offset = 0x00;
constexpr uint8_t table_leaf_payload_length_length = 2;

constexpr uint8_t table_leaf_rowid_offset =
    table_leaf_payload_length_offset + table_leaf_payload_length_length;
constexpr uint8_t table_leaf_rowid_length = 4;

constexpr uint8_t table_leaf_payload_offset =
    table_leaf_rowid_offset + table_leaf_rowid_length;

/* Table B-Tree Interior Cell */
constexpr uint8_t table_interior_left_pointer_offset = 0x00;
constexpr uint8_t table_interior_left_pointer_length = 4;

constexpr uint8_t table_interior_key_offset =
    table_interior_left_pointer_offset + table_interior_left_pointer_length;
constexpr uint8_t table_interior_key_length = 4;

constexpr uint8_t table_interior_cell_length =
    table_interior_key_offset + table_interior_key_length;

}  // namespace internal

#endif  // TINY_BASE_PAGE_FORMAT_H_
