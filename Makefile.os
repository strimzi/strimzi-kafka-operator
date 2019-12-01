FIND = find
SED = sed
GREP = grep
CP = cp
UNIQ = uniq
SORT = sort

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	FIND = gfind
	SED = gsed
	GREP = ggrep
	CP = gcp
	UNIQ = guniq
	SORT = gsort
endif