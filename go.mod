module github.com/longhorn/longhorn-manager

go 1.13

replace (
	k8s.io/api => k8s.io/api v0.0.0-20181005203742-357ec6384fa7
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180913025736-6dd46049f395
	k8s.io/client-go => k8s.io/client-go v0.0.0-20181005204318-cb4883f3dea0
)

require (
	github.com/Microsoft/go-winio v0.4.14 // indirect
	github.com/container-storage-interface/spec v0.3.0
	github.com/coreos/bbolt v1.3.3 // indirect
	github.com/coreos/etcd v3.3.17+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/docker/go-connections v0.2.2-0.20170203235624-7da10c8c50ca
	github.com/evanphx/json-patch v4.5.0+incompatible // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/groupcache v0.0.0-20191002201903-404acd9df4cc // indirect
	github.com/google/btree v1.0.0 // indirect
	github.com/googleapis/gnostic v0.3.1 // indirect
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/gorilla/websocket v1.4.0
	github.com/gregjones/httpcache v0.0.0-20190611155906-901d90724c79 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.11.3 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/kubernetes-csi/drivers v0.4.1
	github.com/kubernetes-incubator/external-storage v5.2.0+incompatible
	github.com/longhorn/backupstore v0.0.0-20191018221817-26f8c517c0c3
	github.com/longhorn/go-iscsi-helper v0.0.0-20191018221834-9c502e2401f4
	github.com/longhorn/longhorn-engine v0.6.3-0.20191019001331-17b9010c3464
	github.com/mitchellh/mapstructure v1.1.2
	github.com/opencontainers/go-digest v1.0.0-rc1 // indirect
	github.com/pborman/uuid v1.2.0 // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.2.1 // indirect
	github.com/rancher/go-rancher v0.1.1-0.20190307222549-9756097e5e4c
	github.com/robfig/cron v1.2.0
	github.com/satori/go.uuid v1.1.1-0.20170321230731-5bf94b69c6b6
	github.com/sirupsen/logrus v1.4.2
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tmc/grpc-websocket-proxy v0.0.0-20190109142713-0ad062ec5ee5 // indirect
	github.com/urfave/cli v1.22.1
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	go.etcd.io/bbolt v1.3.3 // indirect
	golang.org/x/time v0.0.0-20190921001708-c4c64cad1fd0
	google.golang.org/grpc v1.23.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/square/go-jose.v2 v2.3.1 // indirect
	gopkg.in/yaml.v2 v2.2.4
	k8s.io/api v0.0.0-20181005203742-357ec6384fa7
	k8s.io/apiextensions-apiserver v0.0.0-20181005210900-6b7e25efea53
	k8s.io/apimachinery v0.0.0-20180913025736-6dd46049f395
	k8s.io/apiserver v0.0.0-20181005205051-9f398e330d7f // indirect
	k8s.io/client-go v10.0.0+incompatible
	k8s.io/csi-api v0.0.0-20181005211323-acd5d7181032 // indirect
	k8s.io/kube-openapi v0.0.0-20190918143330-0270cf2f1c1d // indirect
	k8s.io/kubernetes v1.12.1
	k8s.io/utils v0.0.0-20191010214722-8d271d903fe4
)
