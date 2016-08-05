#ifndef TINY_BASE_DATABASE_ENGINE_H_
#define TINY_BASE_DATABASE_ENGINE_H_

#include <unordered_map>

#include "sql_command.h"
#include "table_manager.h"

namespace sql {

using TableInfo = std::pair<int32_t, int32_t>;

class DatabaseEngine {
 public:
  DatabaseEngine(void);
  void Run(const std::string file_path);

 private:
  static const std::string hidden_file;
  static const std::string regex_for_name;
  static const std::string regex_for_value;
  static const std::string regex_for_type;
  static const CreateTableCommand root_schema_tables;
  static const CreateTableCommand root_schema_columns;

  std::unordered_map<std::string, internal::TableManager> database_tables_;

  bool Execute(const std::string& sql_command);

  // parser
  bool ParseCreateTableCommand(const std::string& sql_command,
                               CreateTableCommand& command);
  bool ParseInsertIntoCommand(const std::string& sql_command,
                              InsertIntoCommand& command);
  bool ParseSelectFromCommand(const std::string& sql_command,
                              SelectFromCommand& command);
  bool ParseShowTableCommand(const std::string& sql_command);
  bool ParseUpdateSetCommand(const std::string& sql_command,
                             UpdateSetCommand& command);
  bool ParseDropTableCommand(const std::string& sql_command,
                             DropTableCommand& command);

  static bool ParseValue(const std::string& value_str,
                         const SchemaDataType& type,
                         std::vector<std::string>& values);

  static bool IsNotNullViolate(const TypeCode& type_code,
                               const ColumnAttribute& attr);

  // Executor
  bool ExecuteCreateTableCommand(const CreateTableCommand& command);
  void ExecuteInsertIntoCommand(const InsertIntoCommand& command);
  void ExecuteSelectFromCommand(const SelectFromCommand& command);
  void ExecuteShowTablesCommand(void);
  void ExecuteUpdateSetCommand(const UpdateSetCommand& command);
  void ExecuteDropTableCommand(const DropTableCommand& command);

  // Manage table
  void RegisterTable(const CreateTableCommand& table_schema);
  const TableInfo LoadTableInfo(const std::string& table_name);
  const TableInfo LoadRootTableInfo(const std::string& table_name);
  const CreateTableCommand LoadSchema(const std::string& table_name);
  internal::TableManager* LoadTable(const std::string& table_name);
  internal::TableManager* TryLoadTable(const std::string& table_name);
  void UpdateTableInfo(const std::string& table_name);
  void SaveRootTableInfo(void);

  void GetRowid(const std::string& target_table,
                const std::string& condition_table,
                std::vector<int32_t>& rowid);

  const int32_t GetMaxRowid(const std::string& target_table);

  void ClearTableInfo(const std::string& target_table,
                      const std::string& condition_table);

  // helper
  static bool ExtractStr(const std::string& target_str,
                         const std::string& regex_str,
                         std::vector<std::string>& result_str);

  static void SplitStr(const std::string& target_str, const char delimit,
                       std::vector<std::string>& split_str);

  static const bool ExtractStrInQuotation(const std::string& target,
                                          std::string& result_str);
};

}  // namespace sql

#endif  // TINY_BASE_DATABASE_ENGINE_H_
