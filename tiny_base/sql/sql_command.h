#ifndef TINY_BASE_SQL_COMMAND_H_
#define TINY_BASE_SQL_COMMAND_H_

#include <experimental/any>

namespace sql {

enum DataType {
  OneByteNull = 0x00,
  TwoByteNull = 0x01,
  FourByteNull = 0x02,
  EightByteNull = 0x03,
  TinyInt = 0x04,
  SmallInt = 0x05,
  Int = 0x06,
  BigInt = 0x07,
  Real = 0x08,
  Double = 0x09,
  DateTime = 0x0A,
  Date = 0x0B,
  Text = 0x0C
};

enum ColumnAttribute { could_null, not_null, primary_key };

static std::vector<uint16_t> DataTypeSize = {1, 2, 4, 8, 1, 2,
                                             4, 8, 4, 8, 8, 8};

struct CreateTableColumn {
  std::string column_name;
  DataType type;
  ColumnAttribute attribute;
};

struct CreateTableCommand {
  std::string table_name;
  std::vector<CreateTableColumn> column_list;
};

struct InsertIntoCommand {
  std::string table_name;
  std::vector<std::experimental::any> value_list;
};

}  // namespace sql

#endif  // TINY_BASE_SQL_COMMAND_H_
