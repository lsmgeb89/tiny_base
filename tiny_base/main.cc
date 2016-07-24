#include "table_manager.h"

int main(int argc, char* argv[]) {
  /* ----------- System Table ----------- */

  fs::path tables_path("data/tiny_base_tables.tbl");
  fs::path columns_path("data/tiny_base_columns.tbl");

  internal::TableManager tiny_base_tables(tables_path);
  internal::TableManager tiny_base_columns(columns_path);

  sql::CreateTableCommand root_schema_tables = {
      "tiny_base_tables",
      {{"table_id", sql::Int, sql::primary_key},
       {"root_page", sql::Int, sql::not_null},
       {"table_name", sql::Text, sql::not_null}}};
  sql::CreateTableCommand root_schema_columns = {
      "tiny_base_columns",
      {{"column_id", sql::Int, sql::primary_key},
       {"table_name", sql::Text, sql::not_null},
       {"column_name", sql::Text, sql::not_null},
       {"data_type", sql::Int, sql::not_null},
       {"primary_key", sql::TinyInt, sql::not_null},
       {"not_null", sql::TinyInt, sql::not_null}}};

  if (!tiny_base_tables.Exists()) {
    tiny_base_tables.CreateTable(root_schema_tables);
  } else {
    tiny_base_tables.Load(root_schema_tables);
  }

  if (!tiny_base_columns.Exists()) {
    tiny_base_columns.CreateTable(root_schema_columns);
  } else {
    tiny_base_columns.Load(root_schema_columns);
  }

  /* ----------- User Table ----------- */

  fs::path user_table_path("data/greek.tbl");
  internal::TableManager user_table(user_table_path);
  sql::CreateTableCommand c_1 = {"greek",
                                 {{"greek_id", sql::Int, sql::primary_key},
                                  {"letter", sql::Text, sql::not_null}}};

  if (!user_table.Exists()) {
    user_table.CreateTable(c_1);
  } else {
    user_table.Load(c_1);
  }

  char base('a');
  char code;
  std::vector<uint8_t> index = {4, 3, 9, 2, 5, 6, 7, 8, 10, 11, 12, 13, 14};

  for (auto i : index) {
    code = base + i;
    user_table.InsertInto(
        {"greek", {static_cast<int32_t>(i), std::string(114, code)}});
  }

  return 0;
}
