module github.com/longhorn/longhorn-manager

go 1.21

replace (
	k8s.io/api => k8s.io/api v0.28.5
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.28.5
	k8s.io/apimachinery => k8s.io/apimachinery v0.28.5
	k8s.io/apiserver => k8s.io/apiserver v0.28.5
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.28.5
	k8s.io/client-go => k8s.io/client-go v0.28.5
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.28.5
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.28.5
	k8s.io/code-generator => k8s.io/code-generator v0.28.5
	k8s.io/component-base => k8s.io/component-base v0.28.5
	k8s.io/component-helpers => k8s.io/component-helpers v0.28.5
	k8s.io/controller-manager => k8s.io/controller-manager v0.28.5
	k8s.io/cri-api => k8s.io/cri-api v0.28.5
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.28.5
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.28.5
	k8s.io/kms => k8s.io/kms v0.28.5
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.28.5
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.28.5
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.28.5
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.28.5
	k8s.io/kubectl => k8s.io/kubectl v0.28.5
	k8s.io/kubelet => k8s.io/kubelet v0.28.5
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.28.5
	k8s.io/metrics => k8s.io/metrics v0.28.5
	k8s.io/mount-utils => k8s.io/mount-utils v0.28.5
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.28.5
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.28.5
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.28.5
	k8s.io/sample-controller => k8s.io/sample-controller v0.28.5
)

require (
	github.com/container-storage-interface/spec v1.8.0
	github.com/docker/go-connections v0.4.0
	github.com/go-co-op/gocron v1.37.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.1
	github.com/jinzhu/copier v0.3.5
	github.com/kubernetes-csi/csi-lib-utils v0.6.1
	github.com/longhorn/backing-image-manager v1.6.0
	github.com/longhorn/backupstore v0.0.0-20240219094812-3a87ee02df77
	github.com/longhorn/go-common-libs v0.0.0-20240319112414-b75404dc7fbc
	github.com/longhorn/go-iscsi-helper v0.0.0-20240308033847-bc3aab599425
	github.com/longhorn/go-spdk-helper v0.0.0-20240325073158-8d33f2aada15
	github.com/longhorn/longhorn-engine v1.6.0
	github.com/longhorn/longhorn-instance-manager v1.7.0-dev.0.20240308123529-28ec1b59c683
	github.com/longhorn/longhorn-share-manager v1.6.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.17.0
	github.com/rancher/dynamiclistener v0.3.6
	github.com/rancher/go-rancher v0.1.1-0.20220412083059-ff12399dd57b
	github.com/rancher/wrangler v1.1.2
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/urfave/cli v1.22.14
	golang.org/x/mod v0.14.0
	golang.org/x/net v0.22.0
	golang.org/x/sys v0.18.0
	golang.org/x/time v0.3.0
	google.golang.org/grpc v1.62.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.28.6
	k8s.io/apiextensions-apiserver v0.25.4
	k8s.io/apimachinery v0.29.3
	k8s.io/cli-runtime v0.28.5
	k8s.io/client-go v0.28.6
	k8s.io/kubernetes v1.28.5
	k8s.io/metrics v0.28.5
	k8s.io/mount-utils v0.29.3
	k8s.io/utils v0.0.0-20240310230437-4693a0247e57
	sigs.k8s.io/controller-runtime v0.10.1
)

require (
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230305170008-8188dc5388df // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/pprof v0.0.0-20230817174616-7a8ec2ada47b // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/shirou/gopsutil/v3 v3.24.2 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/bbolt v1.3.8 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	golang.org/x/exp v0.0.0-20231219180239-dc181d75b848 // indirect
	golang.org/x/tools v0.16.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240123012728-ef4313101c80 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240123012728-ef4313101c80 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/RoaringBitmap/roaring v1.9.0 // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/c9s/goprocinfo v0.0.0-20210130143923-c95fcf8c64a8 // indirect
	github.com/cenkalti/backoff/v4 v4.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/docker/distribution v2.8.2+incompatible // indirect
	github.com/emicklei/go-restful/v3 v3.9.0 // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/felixge/httpsnoop v1.0.3 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/gammazero/deque v0.2.1 // indirect
	github.com/gammazero/workerpool v1.1.3 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.19.6 // indirect
	github.com/go-openapi/jsonreference v0.20.2 // indirect
	github.com/go-openapi/swag v0.22.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/cel-go v0.16.1 // indirect
	github.com/google/fscrypt v0.3.4 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/gorilla/context v1.1.1 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.11.3 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/longhorn/longhorn-spdk-engine v0.0.0-20240319114738-c9046e18cf8c // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/selinux v1.10.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/rancher/lasso v0.0.0-20240123150939-7055397d6dfa
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.11.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/slok/goresilience v0.2.0 // indirect
	github.com/spf13/cobra v1.7.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.35.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.35.1 // indirect
	go.opentelemetry.io/otel v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/internal/retry v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.10.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.10.0 // indirect
	go.opentelemetry.io/otel/metric v0.31.0 // indirect
	go.opentelemetry.io/otel/sdk v1.10.0 // indirect
	go.opentelemetry.io/otel/trace v1.10.0 // indirect
	go.opentelemetry.io/proto/otlp v0.19.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.24.0 // indirect
	golang.org/x/crypto v0.21.0 // indirect
	golang.org/x/oauth2 v0.16.0 // indirect
	golang.org/x/sync v0.6.0
	golang.org/x/term v0.18.0 // indirect
	golang.org/x/text v0.14.0
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240123012728-ef4313101c80 // indirect
	google.golang.org/protobuf v1.33.0
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	k8s.io/apiserver v0.28.5 // indirect
	k8s.io/cloud-provider v0.0.0 // indirect
	k8s.io/component-base v0.28.5 // indirect
	k8s.io/component-helpers v0.28.5 // indirect
	k8s.io/controller-manager v0.28.5 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.110.1 // indirect
	k8s.io/kms v0.28.5 // indirect
	k8s.io/kube-aggregator v0.25.4 // indirect
	k8s.io/kube-openapi v0.0.0-20230717233707-2695361300d9 // indirect
	k8s.io/kubelet v0.0.0 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.1.2 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.3 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

replace github.com/longhorn/longhorn-instance-manager v1.7.0-dev.0.20240308123529-28ec1b59c683 => github.com/derekbit/longhorn-instance-manager v0.0.0-20240326073731-b2526e3df81e
