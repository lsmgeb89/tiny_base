#ifndef TINY_BASE_PAGE_MANAGER_H_
#define TINY_BASE_PAGE_MANAGER_H_

#include <vector>
#include <utility>
#include "file_util.h"

namespace internal {

using CellKey = int32_t;
using CellKeyRange = std::pair<CellKey, CellKey>;
using PageIndex = uint32_t;
using PageRecord = std::vector<char>;

enum PageType {
  InvalidCell = 0x00,
  IndexInteriorCell = 0x02,
  TableInteriorCell = 0x05,
  IndexLeafCell = 0x0a,
  TableLeafCell = 0x0d
};

class PageManager {
 public:
  PageManager(utils::FileHandle& table_file,
              const utils::FilePosition& page_base);

  // parser
  void ParseInfo(void);

  void UpdateInfo(void);

  // Getter
  uint8_t GetCellNum(void) const { return cell_num_; }

  CellKeyRange GetCellKeyRange(void);

  CellKey GetCellKey(const uint8_t& cell_index);

  PageIndex GetLeftMostPagePointer(void);

  PageIndex GetRightMostPagePointer(void) { return right_most_pointer_; }

  // Setter
  void SetPageType(const PageType& page_type) { page_type_ = page_type; }

  bool IsLeaf(void) const { return (page_type_ | 0x08); }

  bool HasSpace(const utils::FileOffset& record_size) const;

  void InsertRecord(const uint8_t& cell_index, const PageRecord& record);

 private:
  // file related
  utils::FileHandle table_file_;
  utils::FileOffset page_base_;

  // page header
  PageType page_type_;
  uint8_t cell_num_;
  uint16_t cell_content_offset_;
  uint32_t right_most_pointer_;

  // cell pointer array
  std::vector<uint16_t> cell_pointer_array_;
};

}  // namespace internal

#endif  // TINY_BASE_PAGE_MANAGER_H_
