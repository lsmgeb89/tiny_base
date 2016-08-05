#!/bin/bash
#

readonly NAME="cs6360_project_2"
readonly RELEASE_FOLDER="${HOME}/${NAME}"
readonly RELEASE_ZIP="${HOME}/${NAME}.zip"

# delete previous release zip
if [ -f "$RELEASE_ZIP" ]; then
  rm "$RELEASE_ZIP"
fi

mkdir -p "$RELEASE_FOLDER"/src
# copy source files
cp -ar tiny_base/* "$RELEASE_FOLDER"/src
# copy readme.txt
cp readme.txt "$RELEASE_FOLDER"
# package all files
pushd "${HOME}" > /dev/null 2>&1
zip -r "$RELEASE_ZIP" "$NAME"/*
chmod 777 "$RELEASE_ZIP"
popd > /dev/null 2>&1

# delete release folder
if [ -d "$RELEASE_FOLDER" ]; then
  rm -rf "$RELEASE_FOLDER"
fi

