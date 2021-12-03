#! /usr/bin/env bash

jars_dir=$1
classes_root=$2

for jar in $(find $jars_dir -type f -name '*.jar')
do
  extracted=$classes_root/$(basename "$jar")
  mkdir -p "$extracted"
  unzip -qq "$jar" -d "$extracted" >/dev/null
done