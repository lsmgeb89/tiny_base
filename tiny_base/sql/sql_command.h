#ifndef TINY_BASE_SQL_COMMAND_H_
#define TINY_BASE_SQL_COMMAND_H_

#include <experimental/optional>
#include "sql_value.h"

namespace sql {

struct CreateTableColumn {
  std::string column_name;
  SchemaDataType type;
  ColumnAttribute attribute;
};

struct CreateTableCommand {
  std::string table_name;
  std::vector<CreateTableColumn> column_list;
};

using TypeCodeList = std::vector<TypeCode>;
using TypeValue = std::pair<TypeCode, Value>;
using ValueList = std::vector<Value>;
using TypeValueList = std::vector<TypeValue>;

struct InsertIntoCommand {
  std::string table_name;
  TypeCodeList type_list;
  ValueList value_list;
};

struct WhereClause {
  std::string column_name;
  OperatorType condition_operator;
  TypeCode type_code;
  Value value;
};

struct SelectFromCommand {
  std::string table_name;
  std::vector<std::string> column_name;
  std::experimental::optional<WhereClause> where;
};

}  // namespace sql

#endif  // TINY_BASE_SQL_COMMAND_H_
