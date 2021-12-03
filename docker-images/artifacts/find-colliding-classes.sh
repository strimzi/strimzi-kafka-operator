#! /usr/bin/env bash
# $1: Directory containing (directories containing) .class files
unzipped_dir=$1

source $(dirname $(realpath $0))/../../tools/multi-platform-support.sh

# compute list of <md5> <classfile>
find $unzipped_dir ! -empty -type f -name '*.class' -a ! -name 'module-info.class' -exec md5sum {} + | \
  # split into <md5> <jarname> <classname>
  $SED -E 's#([a-z0-9]+).*/([^/]*[.]jar)/(.*)#\1\t\2\t\3#' | \
  # sort by classname
  $SORT -r -k 3 | \
  # find duplicate classname
  $UNIQ -D -f2 | \
  # swap column order => <classname> <jarname> <md5>
  awk '{printf("%s\t%s\t%s\n",$2,$3,$1);}' | \
  # find unique md5 (i.e. classfiles differ)
  $UNIQ -u -f2
