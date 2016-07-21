#!/bin/bash
#
# Format source code with Google C++ style
find . -name '*.cc' -or -name '*.h' | xargs clang-format-3.6 -style=Google -i
