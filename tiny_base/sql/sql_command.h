#ifndef TINY_BASE_SQL_COMMAND_H_
#define TINY_BASE_SQL_COMMAND_H_

#include <experimental/optional>
#include "sql_value.h"

namespace sql {

static std::vector<uint16_t> DataTypeSize = {1, 2, 4, 8, 1, 2,
                                             4, 8, 4, 8, 8, 8};

static uint16_t DataTypeToSize(const int16_t& type) {
  return type < Text ? DataTypeSize[type] : (type - Text);
}

struct CreateTableColumn {
  std::string column_name;
  DataType type;
  ColumnAttribute attribute;
};

struct CreateTableCommand {
  std::string table_name;
  std::vector<CreateTableColumn> column_list;
};

using ValueList = std::vector<Value>;

struct InsertIntoCommand {
  std::string table_name;
  ValueList value_list;
};

struct WhereClause {
  std::string column_name;
  OperatorType condition_operator;
  Value value;
};

struct SelectFromCommand {
  std::string table_name;
  std::vector<std::string> column_name;
  std::experimental::optional<WhereClause> where;
};

}  // namespace sql

#endif  // TINY_BASE_SQL_COMMAND_H_
