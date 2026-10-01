# Release artifacts for the one-line installer (scripts/install.sh): one CLI
# binary per platform plus checksums.txt, ready to attach to a GitHub release.
# The build needs ../pkg through a go.work, as the Dockerfile sets up; run it
# from the orchestrator checkout, where go.work already joins both.
DIST      ?= dist
PLATFORMS ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64
SHA256    := $(shell command -v sha256sum >/dev/null 2>&1 && echo sha256sum || echo shasum -a 256)

.PHONY: release test-install

release:
	rm -rf $(DIST) && mkdir -p $(DIST)
	for p in $(PLATFORMS); do \
		CGO_ENABLED=0 GOOS=$${p%/*} GOARCH=$${p#*/} go build -trimpath -ldflags="-s -w" \
			-o $(DIST)/lightchain-worker-$${p%/*}-$${p#*/} ./cmd/cli || exit 1; \
	done
	cp scripts/install.sh $(DIST)/
	cd $(DIST) && $(SHA256) lightchain-worker-* install.sh > checksums.txt

test-install:
	sh scripts/install_test.sh
