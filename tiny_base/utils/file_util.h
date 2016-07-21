#ifndef TINY_BASE_FILE_UTIL_H_
#define TINY_BASE_FILE_UTIL_H_

#include <fstream>
#include <memory>
#include <string>
#include <experimental/filesystem>

namespace fs = std::experimental::filesystem;

namespace utils {

class FileUtil;

using FileHandle = std::shared_ptr<FileUtil>;
// pos_type is used for absolute positions in the stream
using FilePosition = std::streampos;  // std::fstream::pos_type;
// off_type is used for relative positions
using FileOffset = std::streamoff;  // std::fstream::off_type;

using FileSize = std::streamsize;

class FileUtil {
 public:
  FileUtil(const fs::path& file_path);

  ~FileUtil(void);

  void CreateFile(void);

  FileSize GetFileSize(void);

  void Read(const FilePosition& start_position, char* data_in,
            const FileOffset& length);

  void Write(const FilePosition& start_position, const char* data_out,
             const FileOffset& length);

 private:
  fs::path file_path_;
  std::fstream file_stream_;

  void Open(void);
};

}  // namespace utils

#endif  // TINY_BASE_FILE_UTIL_H_
