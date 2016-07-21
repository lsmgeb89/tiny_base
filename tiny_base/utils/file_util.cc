#include "file_util.h"

namespace utils {

FileUtil::FileUtil(const fs::path& file_path) : file_path_(file_path) {
  Open();
}

FileUtil::~FileUtil(void) { file_stream_.close(); }

void FileUtil::CreateFile(void) {
  if (fs::exists(file_path_)) {
    // TODO: throw exception
  }

  file_stream_.open(file_path_, std::ios::out);
  file_stream_.close();

  Open();
}

void FileUtil::Open(void) {
  if (!fs::exists(file_path_)) {
    // TODO: throw exception
  }

  file_stream_.open(file_path_,
                    (std::ios::binary | std::ios::in | std::ios::out));
}

FileSize FileUtil::GetFileSize(void) {
  file_stream_.ignore(std::numeric_limits<std::streamsize>::max());
  return file_stream_.gcount();
}

void FileUtil::Read(const FilePosition& start_position, char* data_in,
                    const FileOffset& length) {
  // TODO: error handling
  file_stream_.seekg(start_position);
  file_stream_.read(data_in, length);
}

void FileUtil::Write(const FilePosition& start_position, const char* data_out,
                     const FileOffset& length) {
  // TODO: error handling
  file_stream_.seekp(start_position);
  file_stream_.write(data_out, length);
  file_stream_.flush();
}

}  // namespace utils
