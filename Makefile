.PHONY: build test run clean lint verify

BINARY=doris-alert
BUILD_DIR=bin

build:
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(BINARY) ./cmd/
	@echo "✅ built $(BUILD_DIR)/$(BINARY)"

test:
	go test ./... -v

run: build
	./$(BUILD_DIR)/$(BINARY) -c conf/alert.yaml

clean:
	rm -rf $(BUILD_DIR)

lint:
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed"

verify:
	go mod verify
	@echo "✅ go.sum verified"
