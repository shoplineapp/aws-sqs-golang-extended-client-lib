.PHONY: test

format:
	@gofmt -e -s -w -l ./

format_check:
	@golangci-lint run -v --no-config --disable-all -E gofmt

lint:
	@golangci-lint run -v ./... --timeout 3m0s

test:
	@go test ./tests/...