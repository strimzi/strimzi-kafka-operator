#!/bin/bash

fatal=0

function grep_check {
  local pattern=$1
  local description=$2
  local fatalness=${3:-1}
  x=$(grep -i -E -r -n "$pattern" documentation/book/)
  if [ -n "$x" ]; then
    echo "$description:"
    echo "$x"
    y=$(echo "$x" | wc -l)
    ((fatal+=fatalness*y))
  fi
}

# Check for latin abbrevs
grep_check '[^[:alpha:]](e\.g\.|eg)[^[:alpha:]]' "Replace 'e.g'. with 'for example, '"
grep_check '[^[:alpha:]](i\.e\.|ie)[^[:alpha:]]' "Replace 'i.e'. with 'that is, '"
grep_check '[^[:alpha:]]etc\.?[^[:alpha:]]' "Replace 'etc.'. with ' and so on.'"

# And/or
grep_check '[^[:alpha:]]and/or[^[:alpha:]]' "Use either 'and' or 'or', but not 'and/or'"

# Contractions
grep_check '[^[:alpha:]](do|is|are|won|have|ca|does|did|had|has|must)n'"'"'?t[^[:alpha:]]' "Avoid 'nt contraction"
grep_check '[^[:alpha:]]it'"'"'s[^[:alpha:]]' "Avoid it's contraction"
grep_check '[^[:alpha:]]can not[^[:alpha:]]' "Use 'cannot' not 'can not'"

if [ $fatal -gt 0 ]; then
  echo "${fatal} docs problems found."
  exit 1
fi
