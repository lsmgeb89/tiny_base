#include <chrono>
#include <iomanip>
#include <iostream>
#include "sql_command.h"
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
    // tiny_base_tables.Load(root_schema_tables, 255, 0);
  }

  if (!tiny_base_columns.Exists()) {
    tiny_base_columns.CreateTable(root_schema_columns);
  } else {
    // tiny_base_columns.Load(root_schema_columns, 255, 0);
  }

  /* ----------- User Table ----------- */

  fs::path user_table_path("data/greek.tbl");
  internal::TableManager user_table(user_table_path);
  sql::CreateTableCommand c_1 = {"greek",
                                 {{"greek_id", sql::Int, sql::primary_key},
                                  {"id_copy", sql::Int, sql::not_null},
                                  {"real_copy", sql::Double, sql::not_null},
                                  {"time_copy", sql::DateTime, sql::not_null},
                                  {"letter", sql::Text, sql::not_null}}};

  if (!user_table.Exists()) {
    user_table.CreateTable(c_1);
  } else {
    user_table.Load(c_1, 5, 8);
  }

  char base('a');
  char code;
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::time_t test_time;
  std::time_t cond_time =
      std::chrono::system_clock::to_time_t(now - std::chrono::hours(24 * 5));
  std::vector<uint8_t> index = {1, 2, 7, 8, 6, 10, 12, 14, 9, 11, 13, 3, 5, 4};

  std::cout << std::put_time(std::localtime(&cond_time), "%F %T") << '\n';

  for (auto i : index) {
    code = base + i;
    test_time =
        std::chrono::system_clock::to_time_t(now - std::chrono::hours(24 * i));

    std::cout << std::to_string(i)
              << std::put_time(std::localtime(&test_time), ": %F %T") << '\n';

    user_table.InsertInto(
        {"greek",
         {static_cast<int32_t>(i), static_cast<int32_t>(i),
          static_cast<double>(i), static_cast<uint64_t>(test_time),
          std::string(88, code)}});
  }

  // test select
  sql::WhereClause clause = {"greek_id", sql::NotLarger,
                             static_cast<int32_t>(1)};
  std::experimental::optional<sql::WhereClause> where =
      std::experimental::make_optional(clause);
  sql::SelectFromCommand c_2 = {
      "greek",
      {"id_copy", "letter", "time_copy", "real_copy", "greek_id"},
      where};
  std::cout << user_table.SelectFrom(c_2);

  return 0;
}
