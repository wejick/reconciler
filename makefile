# makefile

build:
	go build -o reconciler cmd/reconciler/main.go

run:
	if [ ! -f reconciler ]; then
		make build
	fi
	./reconciler

clean:
	rm report/*
	rm reconciler

setup:
	go get -v ./...