.PHONY: build build-static build-linux build-darwin build-windows test clean

VERSION ?= $(shell git describe --tags 2>/dev/null || echo "dev")
LDFLAGS := -s -w -X main.version=$(VERSION)

# Dev build — dynamically linked, fast
build:
	go build -ldflags="$(LDFLAGS)" -o redmove .

# Static binary — no glibc dependency, safe to copy to any Linux server
build-static:
	CGO_ENABLED=0 go build -ldflags="$(LDFLAGS)" -o redmove .

# Cross-compile targets (static, no CGO)
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o redmove .

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o redmove .

build-windows:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o redmove.exe .

test:
	go test ./... -v

clean:
	rm -rf redmove redmove.exe
