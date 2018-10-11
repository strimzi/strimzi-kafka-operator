#!/bin/bash

fatal=0

function grep_check {
  local pattern=$1
  local description=$2
  local opts=${3:--i -E -r -n}
  local fatalness=${4:-1}
  x=$(grep $opts "$pattern" documentation/book/)
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
grep_check '\<a {ProductPlatformName}' "The article should be 'an' {ProductPlatformName}"
grep_check '\<a {ProductPlatformLongName}' "The article should be 'an' {ProductPlatformLongName}"

# Asciidoc standards
grep_check '[<][<][[:alnum:]_-]+,' "Internal links should be xref:doc_id[Section title], not <<doc_id,link text>>"
grep_check '[[]id=(["'"'"'])[[:alnum:]_-]+(?!-[{]context[}])\1' "[id=...] should end with -{context}" "-i -P -r -n"

# leveloffset=+
grep_check 'leveloffset+=[0-9]+'  "It should be leveloffset=+... not +="

if [ $fatal -gt 0 ]; then
  echo "ERROR: ${fatal} docs problems found."
  exit 1
fi
