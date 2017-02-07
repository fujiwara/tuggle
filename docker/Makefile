.PHONY: clean

all: tuggle goreman docker-compose.yml

clean:
	rm -f tuggle goreman docker-compose.yml

tuggle: ../tuggle.go
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build ..

goreman:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build github.com/mattn/goreman

docker-compose.yml: docker-compose.yml.erb
	erb docker-compose.yml.erb > docker-compose.yml
