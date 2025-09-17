# clean::
# 	rm -r ./data || true
# 	rm -r database.s* || true
# 	rm filin.zip  m3u8_filin m3u8_filin.exe m3u8-filin-win-*.zip || true

# run:: test go_gernerate
# 	go mod tidy
# 	go run cmd/filin/main.go -clean=false -config=./config.yaml  -download=false

# run_download:: test go_gernerate
# 	go mod tidy
# 	go run cmd/filin/main.go -clean=true -config=./config.yaml  -download=true -clean-days=15m

# run_dtmf:: test go_gernerate
# 	go mod tidy
# 	go run cmd/filin/main.go -dtmf-parser=true -clean=true   -config=./config.yaml  -download=true -clean-days=15m

lint:: fmt
	go fmt ./...
	golangci-lint fmt ./...
	golangci-lint run ./...

test:: go_gernerate
	go test ./...

go_update_all::
	go get -u ./...
	go mod tidy

VERSION=$(shell git describe --tags --always)
# pull::
# 	git pull

build::
	CGO_ENABLED=0 \
	GOOS=linux \
	GOARCH=amd64 \
	go build \
		-o redis-cleaner \
		-v \
		main.go

# # // сборка под windows
# build_win::
# 	CGO_ENABLED=1 \
# 	GOOS=windows \
# 	GOARCH=amd64 \
# 	 CC=x86_64-w64-mingw32-gcc \
# 	go build \
#            	-tags windows \
# 			-o m3u8_filin.exe \
# 			-v \
# 			cmd/filin/main.go

# # сборка под MacOS M1 (arm64)
# build_macos_m1::
# 	CGO_ENABLED=0 \
# 	GOOS=darwin \
# 	GOARCH=arm64 \
# 	go build \
# 		-o m3u8_filin_macos_m1 \
# 		-v \
# 		cmd/filin/main.go

# # сборка под MacOS Intel (amd64)
# build_macos_intel::
# 	CGO_ENABLED=0 \
# 	GOOS=darwin \
# 	GOARCH=amd64 \
# 	go build \
# 		-o m3u8_filin_macos_intel \
# 		-v \
# 		cmd/filin/main.go
# zip::
# 	zip filin  m3u8_filin
# zip_win: build_win
# 	zip m3u8-filin-win-$(VERSION).zip  m3u8_filin.exe config.yaml README.md
# zip_macos_m1: build_macos_m1
# 	zip m3u8-filin-macos-m1-$(VERSION).zip m3u8_filin_macos_m1 config.yaml README.md

# zip_macos_intel: build_macos_intel
# 	zip m3u8-filin-macos-intel-$(VERSION).zip m3u8_filin_macos_intel config.yaml README.md


push:: build
	mcli cp redis-cleaner  minio_ott/share/redis-cleaner-$(VERSION)
	# curl  -u admin:Qwerty1@3 --upload-file filin.zip https://nexsus.nsc.iudanet.com/repository/ansible/filin/filin-$(VERSION).zip

# migration_create::
# 	goose -dir internal/repo/migrator/migrations create migration sql

# go_gernerate:
# 	go generate ./...
fmt::
	gofmt -w .
	goimports -w .
	go run golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest -fix ./...
