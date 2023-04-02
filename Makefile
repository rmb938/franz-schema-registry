# Run go fmt against code
fmt:
	go fmt ./...

# Run go vet against code
vet:
	go vet ./...

run: fmt vet
	go run ./main.go

test: fmt vet
	go test ./... -race -covermode=atomic -coverprofile=coverage.out

show-coverage:
	go tool cover -html=coverage.out
