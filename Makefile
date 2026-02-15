BINARY=doris-alert
BUILD_DIR=bin
VERSION=2.0.0

.PHONY: build test clean lint verify run

build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/
	@echo "✅ built $(BUILD_DIR)/$(BINARY)"

test:
	go test ./... -v

run: build
	./$(BUILD_DIR)/$(BINARY) -c conf/config.yaml

clean:
	rm -rf $(BUILD_DIR)

lint:
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed"

verify:
	go mod verify
	@echo "✅ go.sum verified"
