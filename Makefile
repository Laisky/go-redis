install:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.40.1
	go get golang.org/x/tools/cmd/goimports
	go get -u github.com/golang/protobuf/protoc-gen-go@1.3.2

lint:
	goimports -local controller,common -w .
	gofmt -s -w .
	go mod tidy
	golangci-lint run --timeout 3m -E golint,depguard,gocognit,goconst,gofmt,misspell,exportloopref,nilerr #,gosec,lll
