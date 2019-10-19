#!/bin/bash

go get github.com/longhorn/go-iscsi-helper@master
go get github.com/longhorn/backupstore@master
go get github.com/longhorn/longhorn-engine@master
go mod tidy
go mod vendor
