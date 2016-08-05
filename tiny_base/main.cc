#include "database_engine.h"

int main(int argc, char* argv[]) {
  sql::DatabaseEngine engine;

  if (argc == 2) {
    engine.Run(argv[1]);
  } else {
    engine.Run("");
  }

  return 0;
}
