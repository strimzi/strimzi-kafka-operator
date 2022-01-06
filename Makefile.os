FIND = find
SED = sed
GREP = grep
CP = cp
UNIQ = uniq
SORT = sort
SHA1SUM = sha1sum
XARGS = xargs

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	FIND = gfind
	SED = gsed
	GREP = ggrep
	CP = gcp
	UNIQ = guniq
	SORT = gsort
	SHA1SUM = gsha1sum
	XARGS = gxargs
endif