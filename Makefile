COVERAGE_DIR ?= .coverage
.PHONY: test
test: lint
	@find . -name go.mod -execdir go fmt ./... \;
	@find . -mindepth 2 -name go.mod -execdir go test ./... -race \;
	@echo "[go test] running unit tests and collecting coverage metrics"
	@-rm -r $(COVERAGE_DIR)
	@mkdir $(COVERAGE_DIR)
	@go test -v -race -covermode atomic -coverprofile $(COVERAGE_DIR)/combined.txt `go list ./... | grep -v tmp `

.PHONY: lint
lint: lint-check-deps
	@echo "[golangci-lint] linting sources"
	@golangci-lint run ./...

.PHONY: lint-check-deps
lint-check-deps:
	@if [ -z `which golangci-lint` ]; then \
		echo "[go get] installing golangci-lint";\
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.61.0;\
	fi

.PHONY: gen-fb
gen-fb:
	@flatc --go -o pkg/gen/go/fb schema/raft.fbs


