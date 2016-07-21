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

/* Cell Content Format */
constexpr uint32_t page_pointer_length = right_most_pointer_length;

}  // namespace internal

#endif  // TINY_BASE_PAGE_FORMAT_H_
