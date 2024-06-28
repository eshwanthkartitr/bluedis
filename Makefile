build:
	@go build -o bin/Bluedis
run: build
	@./bin/Bluedis
test:
	@go test -v ./...
