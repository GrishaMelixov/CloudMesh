.PHONY: proto build test docker-up docker-down lint

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/logs.proto

build:
	go build ./cmd/...

test:
	go test ./...

lint:
	go vet ./...

docker-up:
	docker-compose up -d

docker-down:
	docker-compose down -v

logs:
	docker-compose logs -f

.DEFAULT_GOAL := build
