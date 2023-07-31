.PHONY: proto
proto: pb/*.proto
	protoc --go_out=pkg --go_opt=paths=source_relative --go-grpc_out=pkg --go-grpc_opt=paths=source_relative $<
	python3 -m grpc_tools.protoc -I. --python_out=./python --grpc_python_out=./python $<

.PHONY: generate
generate:
	go generate ./...

.PHONY: test
test:
	go test -v -race -p 1 -coverprofile=./c.out ./...
