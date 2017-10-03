PROJECT_NAME=kafka-connect-init-loader
REPO=enmasseproject/$(PROJECT_NAME)
DOCKERFILE_DIR ?= .
COMMIT ?= latest
BUILD_IMAGE=golang:1.9-alpine

all: build
	echo "Running Golang build"
	mkdir -p bin
	mkdir -p .go/pkg .go/bin .go/std/amd64
	docker run                                                              \
	    -ti                                                                 \
	    --rm                                                                \
	    -u $$(id -u):$$(id -g)                                              \
		-v "$$(pwd)/.go:/go"                                                \
	    -v "$$(pwd):/go/src/$(PROJECT_NAME)"                                \
	    -v "$$(pwd)/bin:/go/bin"                                            \
		-v "$$(pwd)/.go/std/amd64:/usr/local/go/pkg/linux_amd64_static"     \
	    -w /go/src/$(PROJECT_NAME)                                          \
	    $(BUILD_IMAGE)                                                      \
	    /bin/sh -c "                                                        \
			export CGO_ENABLED=0;                                           \
	        export GOARCH=amd64;                                            \
			ARCH=amd64;                                                     \
			go install -installsuffix \"static\" ./...                      \
		"
	echo "Running docker build"
	curl -s https://raw.githubusercontent.com/EnMasseProject/travis-scripts/master/docker-build.sh | bash /dev/stdin $(REPO) $(DOCKERFILE_DIR)
	curl -s https://raw.githubusercontent.com/EnMasseProject/travis-scripts/master/docker-push.sh | bash /dev/stdin $(REPO)

clean:
	rm -rf .go bin

.PHONY: build clean
