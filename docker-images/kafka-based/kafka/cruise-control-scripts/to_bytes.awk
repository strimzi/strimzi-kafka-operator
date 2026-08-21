BEGIN {
  mult["K"] = 1024
  mult["M"] = 1024 * 1024
  mult["G"] = 1024 * 1024 * 1024
  mult["T"] = 1024 * 1024 * 1024 * 1024
}

{
  s = $0
  sub(/^[ \t]+/, "", s)
  sub(/[ \t]+$/, "", s)
  if (s == "") next

  if (s !~ /^[0-9]+(\.[0-9]+)?([kKmMgGtT])?$/) {
    print "to_bytes: unrecognised size: " $0 > "/dev/stderr"
    exit 1
  }

  sfx = toupper(substr(s, length(s)))
  if (sfx in mult) {
    printf "%.0f\n", int(substr(s, 1, length(s) - 1) * mult[sfx])
  } else {
    printf "%.0f\n", int(s)
  }
}
