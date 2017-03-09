# TinyBase (Database Files and Indexing)

## Summary
  * Built a [CLI][cli] database engine based on a hybrid scheme between [MySQL][mysql] and [SQLite][sqlite] by C++ with [object-oriented programming][oop]
  * Built a hierarchy of tables by using a [MySQL][mysql]-like file-per-table and root-meta-data-table approach
  * Utilized file operations to maintain a simplified [SQLite][sqlite]-like file format of tables for storing data
  * Implemented on-disk [B+ Tree][bptree] to organize fixed-size pages within a table
  * Implemented parsing and executing several [DDL][ddl], [DML][dml] and VDL [SQL][sql] commands including `SHOW` / `CREATE` / `DROP TABLE`, `INSERT INTO` / `UPDATE TABLE` and `SELECT-FROM-WHERE` style query

## Project Information
  * Course: [Database Design (CS 6360)][dd]
  * Professor: [Chris Irwin Davis][chris]
  * Semester: Summer 2016
  * Programming Language: C++
  * Build Tool: [CMake][cmake]

[cli]: https://en.wikipedia.org/wiki/Command-line_interface
[mysql]: https://en.wikipedia.org/wiki/MySQL
[sqlite]: https://en.wikipedia.org/wiki/SQLite
[oop]: https://en.wikipedia.org/wiki/Object-oriented_programming
[bptree]: https://en.wikipedia.org/wiki/B%2B_tree
[ddl]: https://en.wikipedia.org/wiki/Data_definition_language
[dml]: https://en.wikipedia.org/wiki/Data_manipulation_language
[sql]: https://en.wikipedia.org/wiki/SQL
[dd]: https://catalog.utdallas.edu/2016/graduate/courses/cs6360
[chris]: http://cs.utdallas.edu/people/faculty/davis-chris/
[cmake]: https://cmake.org/
