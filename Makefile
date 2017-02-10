.PHONY: test prepare install

test: prepare
	go test -v .

prepare:
	dep ensure

install: prepare
	go get github.com/fujiwara/tuggle
