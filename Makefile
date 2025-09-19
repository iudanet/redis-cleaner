lint:: fmt
	go fmt ./...
	golangci-lint fmt ./...
	golangci-lint run ./...

go_update_all::
	go get -u ./...
	go mod tidy

VERSION=$(shell git describe --tags --always)
build::
	CGO_ENABLED=0 \
	GOOS=linux \
	GOARCH=amd64 \
	go build \
		-o redis-cleaner \
		-v \
		main.go


push:: build
	mcli cp redis-cleaner minio_ott/share/redis-cleaner-$(VERSION)

fmt::
	gofmt -w .
	goimports -w .
	go run golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest -fix ./...
