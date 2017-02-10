CLIENTS := 8
RATE := ""

.PHONY: clean up down all

all: tuggle goreman docker-compose.yml

clean: down
	rm -f tuggle goreman docker-compose.yml

tuggle: ../tuggle.go
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ..

goreman:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build github.com/mattn/goreman

docker-compose.yml: docker-compose.yml.erb
	CLIENTS=$(CLIENTS) RATE=$(RATE) erb docker-compose.yml.erb > docker-compose.yml

up: all
	docker-compose up

down:
	docker-compose down
