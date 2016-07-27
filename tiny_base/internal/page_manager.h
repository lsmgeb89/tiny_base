#ifndef TINY_BASE_PAGE_MANAGER_H_
#define TINY_BASE_PAGE_MANAGER_H_

#include <set>
#include <vector>
#include <utility>
#include "file_util.h"

namespace internal {

using CellIndex = uint32_t;
using CellKey = int32_t;
using CellKeyRange = std::pair<CellKey, CellKey>;
using PageIndex = uint32_t;
using PagePointer = PageIndex;
using PageCell = std::vector<char>;

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

  PageType GetPageType(void) const { return page_type_; }

  CellKeyRange GetCellKeyRange(void);

  std::set<CellKey> GetCellKeySet(void) { return key_set_; }

  CellKey GetCellKey(const CellIndex& cell_index);

  PageCell GetCell(const CellIndex& cell_index);

  PageIndex GetParent(void) { return parent_; }

  PageIndex GetLeftMostPagePointer(void);

  PageIndex GetRightMostPagePointer(void) const { return right_most_pointer_; }

  PagePointer GetCellLeftPointer(const CellIndex& cell_index) const;

  const CellIndex GetLowerBound(const CellKey& key) const {
    return std::distance(key_set_.begin(), key_set_.lower_bound(key));
  }

  // Setter
  void SetPageType(const PageType& page_type) { page_type_ = page_type; }

  void SetParent(const PageIndex& parent_index) { parent_ = parent_index; }

  void SetPageRightMostPointer(const uint32_t& right_most_pointer) {
    right_most_pointer_ = right_most_pointer;
  }

  void SetCellLeftPointer(const CellIndex& cell_index,
                          const PagePointer& left_pointer);

  bool IsLeaf(void) const { return (TableLeafCell == page_type_); }

  bool HasSpace(const utils::FileOffset& cell_size) const;

  void InsertCell(const CellKey& primary_key, const PageCell& cell);

  void DeleteCell(const CellIndex& cell_index);

  void Clear(void);

  void Reset(void);

  void Reorder(void);

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

  // key set
  std::set<CellKey> key_set_;

  // parent
  PageIndex parent_;
};

}  // namespace internal

#endif  // TINY_BASE_PAGE_MANAGER_H_
