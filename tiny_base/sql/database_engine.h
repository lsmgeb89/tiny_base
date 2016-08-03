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
  void Run(void);

 private:
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
  bool ParseShowTable(const std::string& sql_command);

  // Executor
  bool ExecuteCreateTableCommand(const CreateTableCommand& command);
  void ExecuteInsertIntoCommand(const InsertIntoCommand& command);
  void ExecuteSelectFromCommand(const SelectFromCommand& command);
  void ExecuteShowTablesCommand(void);

  // Manage table
  void RegisterTable(const CreateTableCommand& table_schema);
  const TableInfo LoadTableInfo(const std::string& table_name);
  const TableInfo LoadRootTableInfo(const std::string& table_name);
  const CreateTableCommand LoadSchema(const std::string& table_name);
  internal::TableManager* LoadTable(const std::string& table_name);

  // helper
  static bool ExtractStr(const std::string& target_str,
                         const std::string& regex_str,
                         std::vector<std::string>& result_str);

  static void SplitStr(const std::string& target_str, const char delimit,
                       std::vector<std::string>& split_str);
};

}  // namespace sql

#endif  // TINY_BASE_DATABASE_ENGINE_H_
