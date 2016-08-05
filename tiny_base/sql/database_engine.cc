#include <iostream>
#include <regex>
#include <sstream>
#include <vector>

#include "database_engine.h"

namespace sql {

const CreateTableCommand DatabaseEngine::root_schema_tables = {
    "tinybase_tables",
    {{"row_id", Int, primary_key},
     {"table_name", Text, not_null},
     {"root_page", Int, not_null},
     {"fanout", Int, not_null}}};

const CreateTableCommand DatabaseEngine::root_schema_columns = {
    "tinybase_columns",
    {{"row_id", Int, primary_key},
     {"table_name", Text, not_null},
     {"column_name", Text, not_null},
     {"data_type", Text, not_null},
     {"ordinal_position", TinyInt, not_null},
     {"is_nullable", Text, not_null},
     {"column_key", Text, could_null}}};

DatabaseEngine::DatabaseEngine(void) {
  internal::TableManager* tables_manager = nullptr;
  internal::TableManager* columns_manager = nullptr;

  auto res = database_tables_.emplace(
      "tinybase_tables",
      internal::TableManager(fs::path("data/tinybase_tables.tbl")));
  tables_manager = &(res.first->second);

  res = database_tables_.emplace(
      "tinybase_columns",
      internal::TableManager(fs::path("data/tinybase_columns.tbl")));
  columns_manager = &(res.first->second);

  // TODO: check both file exists or not exists (xor)

  if (tables_manager->Exists()) {
    TableInfo info = LoadRootTableInfo("tinybase_tables");
    tables_manager->Load(root_schema_tables, info.first, info.second);
  }

  if (columns_manager->Exists()) {
    TableInfo info = LoadRootTableInfo("tinybase_columns");
    columns_manager->Load(root_schema_columns, info.first, info.second);
  }

  if (!tables_manager->Exists() && !columns_manager->Exists()) {
    tables_manager->CreateTable(root_schema_tables);
    columns_manager->CreateTable(root_schema_columns);
    RegisterTable(root_schema_tables);
    RegisterTable(root_schema_columns);
  }
}

void DatabaseEngine::Run(void) {
  bool exit(false);
  std::string line;
  std::string user_input;
  std::string sql_command;
  std::vector<std::string> res;

  do {
    std::cout << "tinysql> " << std::flush;

    while (std::getline(std::cin, line) && !line.empty()) {
      if (!user_input.empty()) {
        user_input += " ";
      }
      user_input += line;

      auto size = user_input.find_first_of(';');

      // find a semicolon
      if (size != std::string::npos) {
        sql_command = user_input.substr(0, size);
        user_input.erase(0, size + 1);
        if (!ExtractStr(user_input, "^\\s*$", res)) {
          std::cout << std::endl;
        }
        exit = Execute(sql_command);
        if (exit) {
          break;
        }
        if (ExtractStr(user_input, "^\\s*$", res)) {
          std::cout << "tinysql> " << std::flush;
        } else {
          std::cout << "      -> " << std::flush;
        }
      }
    }  // getline loop
  } while (!exit);
}

bool DatabaseEngine::Execute(const std::string& sql_command) {
  bool exit(false);
  bool result;
  std::string keyword;
  std::vector<std::string> token;
  CreateTableCommand create_command;
  InsertIntoCommand insert_command;
  SelectFromCommand select_command;
  UpdateSetCommand update_command;
  DropTableCommand drop_command;

  // get first keyword
  result = ExtractStr(sql_command, "\\s*(\\w+).*", token);
  if (!result || token.size() != 1) {
    goto done;
  }
  keyword = token.front();
  transform(keyword.begin(), keyword.end(), keyword.begin(), ::toupper);

  // parse
  if (keyword == "CREATE") {
    result = ParseCreateTableCommand(sql_command, create_command);
    if (!result) {
      goto done;
    }
    ExecuteCreateTableCommand(create_command);
    UpdateTableInfo("tinybase_tables");
    UpdateTableInfo("tinybase_columns");
  } else if (keyword == "INSERT") {
    result = ParseInsertIntoCommand(sql_command, insert_command);
    if (!result) {
      goto done;
    }
    ExecuteInsertIntoCommand(insert_command);
    UpdateTableInfo(insert_command.table_name);
  } else if (keyword == "SELECT") {
    result = ParseSelectFromCommand(sql_command, select_command);
    if (!result) {
      goto done;
    }
    ExecuteSelectFromCommand(select_command);
  } else if (keyword == "SHOW") {
    result = ParseShowTableCommand(sql_command);
    if (!result) {
      goto done;
    }
    ExecuteShowTablesCommand();
  } else if (keyword == "UPDATE") {
    result = ParseUpdateSetCommand(sql_command, update_command);
    if (!result) {
      goto done;
    }
    ExecuteUpdateSetCommand(update_command);
  } else if (keyword == "DROP") {
    result = ParseDropTableCommand(sql_command, drop_command);
    if (!result) {
      goto done;
    }
    ExecuteDropTableCommand(drop_command);
  } else if (keyword == "EXIT") {
    std::cout << "Bye!" << std::endl;
    SaveRootTableInfo();
    exit = true;
  }

done:
  return exit;
}

bool DatabaseEngine::ParseCreateTableCommand(const std::string& sql_command,
                                             CreateTableCommand& command) {
  bool result(false);
  bool is_nullable(false);
  std::string temp;
  SchemaDataType type;
  CreateTableColumn column;
  std::vector<std::string> token;
  std::vector<std::string> columns;

  // extract table name and remaining
  result =
      ExtractStr(sql_command,
                 "\\s*CREATE\\s*TABLE\\s*(\\w+)\\s*\\((.+)(?=\\))\\s*", token);

  if (!result || token.size() != 2) {
    goto done;
  }

  command.table_name = token.front();

  // split remaining by comma
  temp = token.at(1);
  SplitStr(temp, ',', token);
  if (!token.size()) {
    goto done;
  }

  // first column must be primary key
  result = ExtractStr(token.front(), "\\s*(\\w+)\\s*INT\\s*PRIMARY\\s*KEY\\s*",
                      columns);
  if (!result || columns.size() != 1) {
    goto done;
  }

  column = {columns.front(), Int, primary_key};
  command.column_list.push_back(column);

  // remaining column
  for (auto i = 1; i < token.size(); i++) {
    // first try with NOT NULL
    result = ExtractStr(token.at(i), "\\s*(\\w+)\\s*(\\w+)\\s*NOT\\s*NULL\\s*",
                        columns);

    if (!result || columns.size() != 2) {
      // second try
      result = ExtractStr(token.at(i), "\\s*(\\w+)\\s*(\\w+)\\s*", columns);
      if (!result || columns.size() != 2) {
        goto done;
      } else {
        is_nullable = true;
      }
    } else {
      is_nullable = false;
    }

    // check type
    type = StringToSchemaDataType(columns.at(1));
    if (type == InvalidType) {
      goto done;
    }

    // save column
    column = {columns.front(), type, is_nullable ? could_null : not_null};
    command.column_list.push_back(column);
  }

done:
  return result;
}

bool DatabaseEngine::ParseInsertIntoCommand(const std::string& sql_command,
                                            InsertIntoCommand& command) {
  bool result(false);
  std::string temp;
  TypeCode type_code;
  Value sql_value;
  fs::path file_path;
  internal::TableManager* table(nullptr);
  std::vector<std::string> token;
  std::vector<std::string> values;

  // Extract table name and remaining
  result = ExtractStr(
      sql_command,
      "\\s*INSERT\\s*INTO\\s*TABLE\\s*(\\w+)\\s*VALUES\\s*\\((.+?)\\)\\s*",
      token);
  if (!result || token.size() != 2) {
    goto done;
  }

  // try to load table from memory
  command.table_name = token.front();
  if (database_tables_.find(command.table_name) == database_tables_.end()) {
    // check file
    file_path = "data/" + command.table_name + ".tbl";
    if (fs::exists(file_path)) {
      table = LoadTable(command.table_name);
    } else {
      goto done;
    }
  } else {
    table = &(database_tables_.at(command.table_name));
  }

  temp = token.at(1);
  // split remaining by comma
  SplitStr(temp, ',', token);
  if (!token.size()) {
    goto done;
  }

  // check value
  for (auto i = 0; i < token.size(); i++) {
    // string type
    if (table->GetColumnType(i) == Text) {
      auto str_begin(token.at(i).find_first_of('\''));
      auto str_end(token.at(i).find_last_of('\''));
      if (str_begin == std::string::npos || str_end == std::string::npos) {
        goto done;
      }
      values.clear();
      values.push_back(
          token.at(i).substr(str_begin + 1, str_end - str_begin - 1));
    } else {
      result = ExtractStr(token.at(i), "\\s*([\\w-_:\\.]+)\\s*", values);
      if (!result || values.size() != 1) {
        goto done;
      }
    }

    type_code = DataTypeToTypeCode(table->GetColumnType(i), values.front());
    command.type_list.push_back(type_code);
    sql_value = StringToValue(values.front(), type_code);
    command.value_list.push_back(sql_value);
  }

done:
  return result;
}

bool DatabaseEngine::ParseSelectFromCommand(const std::string& sql_command,
                                            SelectFromCommand& command) {
  bool result(false);
  bool with_where(false);
  std::string temp;
  fs::path file_path;
  OperatorType op;
  TypeCode type_code;
  Value sql_value;
  WhereClause clause;
  std::vector<std::string> condition_str;
  std::vector<std::string> token;
  std::vector<std::string> columns;
  internal::TableManager* table(nullptr);

  // separate them
  result = ExtractStr(sql_command,
                      "\\s*SELECT\\s*(.*?)\\s*FROM\\s*(\\w+)\\s*WHERE\\s*(\\w+)"
                      "\\s*([>=<]{1,2})(.+)",
                      token);
  if (!result || token.size() != 5) {
    result = ExtractStr(sql_command,
                        "\\s*SELECT\\s*(.*?)\\s*FROM\\s*(\\w+)\\s*", token);
    if (!result || token.size() != 2) {
      goto done;
    } else {
      with_where = false;
    }
  } else {
    with_where = true;
  }

  // check table name
  command.table_name = token.at(1);
  if (database_tables_.find(command.table_name) == database_tables_.end()) {
    // check file
    file_path = "data/" + command.table_name + ".tbl";
    if (fs::exists(file_path)) {
      table = LoadTable(command.table_name);
    } else {
      goto done;
    }
  } else {
    table = &(database_tables_.at(command.table_name));
  }

  if (with_where) {
    // check column name
    if (!table->IsColumnValid(token.at(2))) {
      goto done;
    }

    // check operator type
    op = StringToOperator(token.at(3));
    if (op == InvalidOp) {
      goto done;
    }

    if (table->GetColumnType(token.at(2)) == Text) {
      auto str_begin(token.at(4).find_first_of('\''));
      auto str_end(token.at(4).find_last_of('\''));
      if (str_begin == std::string::npos || str_end == std::string::npos) {
        goto done;
      }
      condition_str.push_back(
          token.at(4).substr(str_begin + 1, str_end - str_begin - 1));
    } else {
      result = ExtractStr(token.at(4), "\\s*([\\w-_\\.]+)\\s*", condition_str);
      if (!result || condition_str.size() != 1) {
        goto done;
      }
    }

    // convert value
    type_code = DataTypeToTypeCode(table->GetColumnType(token.at(2)),
                                   condition_str.front());
    sql_value = StringToValue(condition_str.front(), type_code);

    // save whole where clause
    clause = {token.at(2), op, type_code, sql_value};
    command.where = std::experimental::make_optional(clause);
  }

  // split column
  temp = token.front();
  SplitStr(temp, ',', token);

  // SELECT *
  if (token.size() == 1 && token.front() == "*") {
    command.column_name.push_back(token.front());
  } else {
    for (auto i = 0; i < token.size(); i++) {
      result = ExtractStr(token.at(i), "\\s*(\\w+)\\s*", columns);
      if (!result || columns.size() != 1 ||
          !table->IsColumnValid(columns.at(0))) {
        goto done;
      }

      command.column_name.push_back(columns.front());
    }
  }

done:
  return result;
}

bool DatabaseEngine::ParseShowTableCommand(const std::string& sql_command) {
  bool result(false);
  std::vector<std::string> token;
  result = ExtractStr(sql_command, "\\s*SHOW\\s*TABLES\\s*", token);
  return (result && token.empty());
}

bool DatabaseEngine::ParseUpdateSetCommand(const std::string& sql_command,
                                           UpdateSetCommand& command) {
  bool result(false);
  std::string temp;
  fs::path file_path;
  TypeCode type_code;
  Value sql_value;
  std::vector<std::string> condition_str;
  std::vector<std::string> token;
  std::vector<std::string> set_clause;
  internal::TableManager* table(nullptr);

  result = ExtractStr(sql_command,
                      "\\s*UPDATE\\s*([\\.\\w_-]+)\\s*SET\\s*(.+)\\s*WHERE\\s*("
                      "[\\.\\w_-]+)\\s*=(.+)",
                      token);
  if (!result || token.size() != 4) {
    goto done;
  }

  // check table name
  command.table_name = token.front();
  if (database_tables_.find(command.table_name) == database_tables_.end()) {
    // check file
    file_path = "data/" + command.table_name + ".tbl";
    if (fs::exists(file_path)) {
      table = LoadTable(command.table_name);
    } else {
      goto done;
    }
  } else {
    table = &(database_tables_.at(command.table_name));
  }

  // check column name
  if (!table->IsColumnValid(token.at(2))) {
    goto done;
  }

  if (table->GetColumnType(token.at(2)) == Text) {
    auto str_begin(token.at(3).find_first_of('\''));
    auto str_end(token.at(3).find_last_of('\''));
    if (str_begin == std::string::npos || str_end == std::string::npos) {
      goto done;
    }
    condition_str.clear();
    condition_str.push_back(
        token.at(3).substr(str_begin + 1, str_end - str_begin - 1));
  } else {
    result = ExtractStr(token.at(3), "\\s*([\\w-_\\.]+)\\s*", condition_str);
    if (!result || condition_str.size() != 1) {
      goto done;
    }
  }

  // convert value
  type_code = DataTypeToTypeCode(table->GetColumnType(token.at(2)),
                                 condition_str.front());
  sql_value = StringToValue(condition_str.front(), type_code);

  // save whole where clause
  command.where = {token.at(2), Equal, type_code, sql_value};

  // split column
  temp = token.at(1);
  SplitStr(temp, ',', token);

  // set clause
  for (auto i = 0; i < token.size(); i++) {
    result = ExtractStr(token.at(i), "\\s*([\\.\\w_-]+)\\s*=(.+)", set_clause);
    if (!result || set_clause.size() != 2 ||
        !table->IsColumnValid(set_clause.front())) {
      goto done;
    }

    // first try string
    if (table->GetColumnType(set_clause.at(0)) == Text) {
      auto str_begin(set_clause.at(1).find_first_of('\''));
      auto str_end(set_clause.at(1).find_last_of('\''));
      if (str_begin == std::string::npos || str_end == std::string::npos) {
        goto done;
      }
      condition_str.clear();
      condition_str.push_back(
          set_clause.at(1).substr(str_begin + 1, str_end - str_begin - 1));
    } else {
      result =
          ExtractStr(set_clause.at(1), "\\s*([\\w-_\\.]+)\\s*", condition_str);
      if (!result || condition_str.size() != 1) {
        goto done;
      }
    }

    // convert value
    type_code = DataTypeToTypeCode(table->GetColumnType(set_clause.front()),
                                   condition_str.front());
    sql_value = StringToValue(condition_str.front(), type_code);

    command.set_list.push_back({set_clause.front(), type_code, sql_value});
  }

done:
  return result;
}

bool DatabaseEngine::ParseDropTableCommand(const std::string& sql_command,
                                           DropTableCommand& command) {
  bool result(false);
  std::vector<std::string> token;

  result =
      ExtractStr(sql_command, "\\s*DROP\\s*TABLE\\s*([\\.\\w_-]+)\\s*", token);
  if (!result || token.size() != 1) {
    goto done;
  }

  // check table
  command.table_name = token.front();
  if ((database_tables_.find(command.table_name) == database_tables_.end()) &&
      !fs::exists("data/" + command.table_name + ".tbl")) {
    result = false;
  }

done:
  return result;
}

bool DatabaseEngine::ExecuteCreateTableCommand(
    const CreateTableCommand& command) {
  bool result(false);
  std::string file_name = "data/" + command.table_name + ".tbl";
  fs::path file_path(file_name);

  if (fs::exists(file_path)) {
    return false;
  }

  auto res = database_tables_.emplace(command.table_name,
                                      internal::TableManager(file_path));
  res.first->second.CreateTable(command);

  RegisterTable(command);

done:
  return result;
}

void DatabaseEngine::ExecuteInsertIntoCommand(
    const InsertIntoCommand& command) {
  database_tables_.at(command.table_name).InsertInto(command);
}

void DatabaseEngine::ExecuteSelectFromCommand(
    const SelectFromCommand& command) {
  std::cout << database_tables_.at(command.table_name)
                   .SelectFrom(command)
                   .second << std::flush;
}

void DatabaseEngine::ExecuteShowTablesCommand(void) {
  SelectFromCommand show_tables = {"tinybase_tables", {"table_name"}};
  ExecuteSelectFromCommand(show_tables);
}

void DatabaseEngine::ExecuteUpdateSetCommand(const UpdateSetCommand& command) {
  std::cout << database_tables_.at(command.table_name).UpdateSet(command)
            << std::flush;
}

void DatabaseEngine::ExecuteDropTableCommand(const DropTableCommand& command) {
  ClearTableInfo(root_schema_tables.table_name, command.table_name);
  ClearTableInfo(root_schema_columns.table_name, command.table_name);
  fs::remove("data/" + command.table_name + ".tbl");
}

void DatabaseEngine::SplitStr(const std::string& target_str, const char delimit,
                              std::vector<std::string>& split_str) {
  std::stringstream str_stream(target_str);
  std::string unit_str;

  split_str.clear();

  // TODO: check failbit
  while (getline(str_stream, unit_str, delimit)) {
    split_str.push_back(unit_str);
  }
}

bool DatabaseEngine::ExtractStr(const std::string& target_str,
                                const std::string& regex_str,
                                std::vector<std::string>& result_str) {
  bool result(false);
  // without regard to case
  std::regex rgx(regex_str, std::regex::icase);
  std::smatch match_result;

  result_str.clear();

  result = std::regex_search(target_str.begin(), target_str.end(), match_result,
                             rgx);
  if (result) {
    for (auto i = 1; i < match_result.size(); i++) {
      result_str.push_back(match_result[i]);
    }
  }

  return result;
}

const int32_t DatabaseEngine::GetMaxRowid(const std::string& target_table) {
  SelectFromCommand query_rowid_count = {target_table, {"table_name"}};

  auto res =
      database_tables_.at(target_table).InternalSelectFrom(query_rowid_count);
  return res.size();
}

void DatabaseEngine::GetRowid(const std::string& target_table,
                              const std::string& condition_table,
                              std::vector<int32_t>& rowid) {
  std::vector<sql::TypeValueList> tables_query_result;
  WhereClause where_condition = {
      "table_name", Equal, static_cast<TypeCode>(Text + condition_table.size()),
      condition_table};

  SelectFromCommand query_rowid = {
      target_table,
      {"row_id"},
      std::experimental::make_optional(where_condition)};

  tables_query_result = database_tables_.at(query_rowid.table_name)
                            .InternalSelectFrom(query_rowid);

  rowid.clear();

  for (auto tuple_result : tables_query_result) {
    int32_t row_id = expr::any_cast<int32_t>(tuple_result.front().second);
    rowid.push_back(row_id);
  }
}

void DatabaseEngine::ClearTableInfo(const std::string& target_table,
                                    const std::string& condition_table) {
  std::vector<int32_t> rowid_list;
  int32_t max_row_id;
  DeleteFromCommand delete_record;
  UpdateSetCommand update_rowid;

  max_row_id = GetMaxRowid(target_table);
  GetRowid(target_table, condition_table, rowid_list);

  // do clearance
  for (auto rowid : rowid_list) {
    delete_record = {target_table, {"row_id", Equal, Int, rowid}};
    database_tables_.at(target_table).DeleteFrom(delete_record);
  }

  // update rowid after deleted tuples
  int32_t max_del_rowid = rowid_list.back();
  if (max_del_rowid < max_row_id) {
    for (auto i = 0; i < max_row_id - rowid_list.back(); i++) {
      update_rowid = {target_table,
                      {{"row_id", Int, rowid_list.at(i)}},
                      {"row_id", Equal, Int, max_del_rowid + i + 1}};
      database_tables_.at(target_table).UpdateSet(update_rowid);
    }
  }
}

void DatabaseEngine::RegisterTable(const CreateTableCommand& table_schema) {
  InsertIntoCommand insert_tables;
  InsertIntoCommand insert_columns;
  SelectFromCommand query_tables_rowid = {"tinybase_tables", {"table_name"}};
  SelectFromCommand query_columns_rowid = {"tinybase_columns", {"table_name"}};

  // row id
  auto res = database_tables_.at(query_tables_rowid.table_name)
                 .SelectFrom(query_tables_rowid);
  int32_t tables_row_id = res.first;

  res = database_tables_.at(query_columns_rowid.table_name)
            .SelectFrom(query_columns_rowid);
  int32_t columns_row_id = res.first;

  // tables
  insert_tables = {
      "tinybase_tables",
      {Int, static_cast<TypeCode>(Text + table_schema.table_name.size()), Int,
       Int},
      {++tables_row_id, table_schema.table_name, static_cast<int32_t>(0),
       static_cast<int32_t>(std::numeric_limits<int32_t>::max())}};
  database_tables_.at(insert_tables.table_name).InsertInto(insert_tables);

  // columns
  std::string table_name;
  std::string column_name;
  std::string data_type;
  std::string column_key;
  std::string is_nullable;
  for (auto i = 0; i < table_schema.column_list.size(); i++) {
    table_name = table_schema.table_name;
    column_name = table_schema.column_list.at(i).column_name;
    data_type = DataTypeToString(table_schema.column_list.at(i).type);
    column_key = IsAttributePrimary(table_schema.column_list.at(i).attribute);
    is_nullable = IsAttributeNullable(table_schema.column_list.at(i).attribute);
    insert_columns = {"tinybase_columns",
                      {Int, static_cast<TypeCode>(Text + table_name.size()),
                       static_cast<TypeCode>(Text + column_name.size()),
                       static_cast<TypeCode>(Text + data_type.size()), TinyInt,
                       static_cast<TypeCode>(Text + is_nullable.size()),
                       static_cast<TypeCode>(Text + column_key.size())},
                      {++columns_row_id, table_name, column_name, data_type,
                       static_cast<int8_t>(i + 1), is_nullable, column_key}};
    database_tables_.at(insert_columns.table_name).InsertInto(insert_columns);
  }
}

internal::TableManager* DatabaseEngine::LoadTable(
    const std::string& table_name) {
  TableInfo table_info = LoadTableInfo(table_name);
  sql::CreateTableCommand table_schema = LoadSchema(table_name);

  // load table
  fs::path file_path = "data/" + table_name + ".tbl";
  auto res =
      database_tables_.emplace(table_name, internal::TableManager(file_path));
  res.first->second.Load(table_schema, table_info.first, table_info.second);

  return &(res.first->second);
}

const std::pair<int32_t, int32_t> DatabaseEngine::LoadTableInfo(
    const std::string& table_name) {
  sql::CreateTableCommand table_schema;
  std::vector<sql::TypeValueList> tables_query_result;

  // query info from root tables
  WhereClause where_condition = {
      "table_name", Equal, static_cast<TypeCode>(Text + table_name.size()),
      table_name};
  SelectFromCommand query_tables_info = {
      "tinybase_tables",
      {"*"},
      std::experimental::make_optional(where_condition)};

  tables_query_result = database_tables_.at("tinybase_tables")
                            .InternalSelectFrom(query_tables_info);

  // fanout
  int32_t root_page =
      expr::any_cast<int32_t>(tables_query_result.front().at(2).second);
  // root page
  int32_t fanout =
      expr::any_cast<int32_t>(tables_query_result.front().at(3).second);

  return std::make_pair(root_page, fanout);
}

const CreateTableCommand DatabaseEngine::LoadSchema(
    const std::string& table_name) {
  sql::CreateTableCommand table_schema;
  std::vector<sql::TypeValueList> columns_query_result;

  // query info from root tables
  WhereClause where_condition = {
      "table_name", Equal, static_cast<TypeCode>(Text + table_name.size()),
      table_name};
  SelectFromCommand query_columns_schema = {
      "tinybase_columns",
      {"*"},
      std::experimental::make_optional(where_condition)};

  columns_query_result = database_tables_.at("tinybase_columns")
                             .InternalSelectFrom(query_columns_schema);

  // form schema
  table_schema.table_name = table_name;
  CreateTableColumn column;
  std::string attribute;
  for (auto result_tuple : columns_query_result) {
    column.column_name = expr::any_cast<std::string>(result_tuple.at(2).second);
    column.type = StringToSchemaDataType(
        expr::any_cast<std::string>(result_tuple.at(3).second));
    // first check column_key
    attribute = expr::any_cast<std::string>(result_tuple.at(6).second);
    if (attribute == "PRI") {
      column.attribute = primary_key;
    } else {
      // second check is_nullable
      attribute = expr::any_cast<std::string>(result_tuple.at(5).second);
      column.attribute = (attribute == "YES") ? could_null : not_null;
    }
    table_schema.column_list.push_back(column);
  }

  return table_schema;
}

const TableInfo DatabaseEngine::LoadRootTableInfo(
    const std::string& table_name) {
  int32_t root_page(0);
  int32_t fanout(0);
  std::ifstream infile("data/.table_info");

  if (table_name == "tinybase_tables") {
    infile >> root_page >> fanout;
  } else if (table_name == "tinybase_columns") {
    infile.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    infile >> root_page >> fanout;
  }

  infile.close();

  return std::make_pair(root_page, fanout);
}

void DatabaseEngine::UpdateTableInfo(const std::string& table_name) {
  // table must be loaded
  if (database_tables_.find(table_name) == database_tables_.end()) {
    return;
  }

  // get current info
  internal::TableManager* table = &(database_tables_.at(table_name));
  int32_t root_page = table->GetRootPage();
  int32_t fanout = table->GetFanout();

  // query table's row_id
  std::vector<sql::TypeValueList> tables_query_result;
  WhereClause where_condition = {
      "table_name", Equal, static_cast<TypeCode>(Text + table_name.size()),
      table_name};

  SelectFromCommand query_rowid = {
      "tinybase_tables",
      {"row_id"},
      std::experimental::make_optional(where_condition)};

  tables_query_result =
      database_tables_.at("tinybase_tables").InternalSelectFrom(query_rowid);

  int32_t row_id =
      expr::any_cast<int32_t>(tables_query_result.front().front().second);

  // update info
  where_condition = {"row_id", Equal, Int, row_id};
  UpdateSetCommand update_command = {
      "tinybase_tables",
      {{"root_page", Int, root_page}, {"fanout", Int, fanout}},
      where_condition};

  database_tables_.at("tinybase_tables").UpdateSet(update_command);
}

void DatabaseEngine::SaveRootTableInfo(void) {
  int32_t root_page;
  int32_t fanout;

  // overwrite existed one
  std::ofstream outfile("data/.table_info");

  root_page = database_tables_.at("tinybase_tables").GetRootPage();
  fanout = database_tables_.at("tinybase_tables").GetFanout();
  outfile << root_page << " " << fanout << "\n";

  root_page = database_tables_.at("tinybase_columns").GetRootPage();
  fanout = database_tables_.at("tinybase_columns").GetFanout();
  outfile << root_page << " " << fanout << "\n";

  outfile.close();
}

}  // namespace sql
