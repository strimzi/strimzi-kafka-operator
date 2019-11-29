#! /usr/bin/env bash
# $1: Directory containing (directories containing) .class files
unzipped_dir=$1

# compute list of <md5> <classfile>
find $unzipped_dir ! -empty -type f -name '*.class' -a ! -name 'module-info.class' -exec md5sum {} + | \
  # split into <md5> <jarname> <classname>
  sed -E 's#([a-z0-9]+).*/([^/]*[.]jar)/(.*)#\1\t\2\t\3#' | \
  # sort by classname
  sort -r -k 3 | \
  # find duplicate classname
  uniq -D -f2 | \
  # swap column order => <classname> <jarname> <md5>
  awk '{printf("%s\t%s\t%s\n",$2,$3,$1);}' | \
  # find unique md5 (i.e. classfiles differ)
  uniq -u -f2