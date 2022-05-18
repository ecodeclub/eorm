test:
	@go test -race ./...

setup:
	sh ./script/setup.sh

lint:
	golangci-lint run