# tuggle

Distributed file mirroring proxy in Consul cluster

## Install

```
go get -u github.com/fujiwara/tuggle
```

```
Usage of tuggle:
  -data-dir string
    	data directory (default "./data")
  -fetch-rate string
    	Max fetch rate limit(/sec) (default "unlimited")
  -fetch-timeout string
    	fetch timeout (default "10m0s")
  -namespace string
    	namespace (default "tuggle")
  -port int
    	listen port (default 8900)
  -slave
    	slave mode (fetch only)
```

## Getting Started

### Run

Run `tuggle` process in a Consul cluster. (e.g. node1, node2, node3)

```
[node{1,2,3}]$ mkdir /tmp/tuggle
[node{1,2,3}]$ tuggle -data-dir /tmp/tuggle
```

- comunicate with consul agent via 127.0.0.1:8500.
- store files in -data-dir.
- listen port tcp/8900

### Put file

tuggle accepts files by HTTP POST method.

```
[node1]$ curl -XPUT -H"Content-Type: application/gzip" --data-binary @test.gz localhost:8900/test.gz
```

List stored files.

```
[node1]$ curl -s localhost:8900 | jq .
[
  {
    "id": "6524a9d7b3bde0f3543f1ead0ae8604f",
    "name": "test.gz",
    "content_type": "application/gzip",
    "size": 8764510,
    "created_at": "2017-02-13T07:46:18.809409122Z"
  }
]
```

ID is md5 hex of filename.

tuggle will register to Consul service(name=tuggle, tag=ID).

```
$ curl -s localhost:8500/v1/catalog/service/tuggle?tag=6524a9d7b3bde0f3543f1ead0ae8604f | jq .
[
  {
    "ID": "ec22d2d8-0728-406b-6212-02e55c5c14a0",
    "Node": "ip-172-31-17-183",
    "Address": "172.31.17.183",
    "TaggedAddresses": {
      "lan": "172.31.17.183",
      "wan": "172.31.17.183"
    },
    "NodeMeta": {},
    "ServiceID": "tuggle",
    "ServiceName": "tuggle",
    "ServiceTags": [
      "6524a9d7b3bde0f3543f1ead0ae8604f"
    ],
    "ServiceAddress": "",
    "ServicePort": 8900,
    "ServiceEnableTagOverride": false,
    "CreateIndex": 10055,
    "ModifyIndex": 46156
  }
]
```

### Get files from other nodes

```
[node2]$ curl --head localhost:8900/test.gz
HTTP/1.1 200 OK
Content-Length: 8764510
Content-Type: application/gzip
Last-Modified: Mon, 13 Feb 2017 07:46:18 UTC
X-Tuggle-Object-Id: 6524a9d7b3bde0f3543f1ead0ae8604f
Date: Mon, 13 Feb 2017 07:53:46 GMT
```

```
[node2]$ curl localhost:8900/test.gz > test.gz
[node2]$ ls -l test.gz
-rw-rw-r-- 1 foo bar 8764510 Feb 13 07:55 test.gz
```

tuggle on node{2,3} works as below.
- Find test.gz in data-dir
- If it is not found
  - Fetch test.gz from `[MD5 of filename].tuggle.service.consul`
  - Store into data-dir.
  - Register myself to Consul service.
  - Serve file to http client.

### Delete files

```
[node1]$ curl -X DELETE localhost:8900/test.gz
```

DELETE is enable on any nodes.

## LICENSE

MIT License

Copyright (c) 2017 FUJIWARA Shunichiro / KAYAC Inc.
