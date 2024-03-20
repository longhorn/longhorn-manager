module github.com/longhorn/longhorn-manager

go 1.21

replace (
	k8s.io/api => k8s.io/api v0.23.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.23.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.23.6
	k8s.io/apiserver => k8s.io/apiserver v0.23.6
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.23.6
	k8s.io/client-go => k8s.io/client-go v0.23.6
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.23.6
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.23.6
	k8s.io/code-generator => k8s.io/code-generator v0.23.6
	k8s.io/component-base => k8s.io/component-base v0.23.6
	k8s.io/component-helpers => k8s.io/component-helpers v0.23.6
	k8s.io/controller-manager => k8s.io/controller-manager v0.23.6
	k8s.io/cri-api => k8s.io/cri-api v0.23.6
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.23.6
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.23.6
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.23.6
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.23.6
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.23.6
	k8s.io/kubectl => k8s.io/kubectl v0.23.6
	k8s.io/kubelet => k8s.io/kubelet v0.23.6
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.23.6
	k8s.io/metrics => k8s.io/metrics v0.23.6
	k8s.io/mount-utils => k8s.io/mount-utils v0.23.6
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.23.6
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.23.6
)

require (
	github.com/container-storage-interface/spec v1.5.0
	github.com/docker/go-connections v0.4.0
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.5.0
	github.com/jinzhu/copier v0.3.5
	github.com/kubernetes-csi/csi-lib-utils v0.6.1
<<<<<<< HEAD
	github.com/longhorn/backing-image-manager v0.0.0-20220609065820-a08f7f47442f
	github.com/longhorn/backupstore v0.0.0-20230606110118-7c0666bcb0cf
	github.com/longhorn/go-iscsi-helper v0.0.0-20231113050545-9df1e6b605c7
	github.com/longhorn/longhorn-engine v1.4.4-rc2
	github.com/longhorn/longhorn-instance-manager v1.4.5-0.20240123080948-c39a4cac7a20
	github.com/longhorn/longhorn-share-manager v1.4.5-0.20231115141251-cb28f2149b7c
=======
	github.com/longhorn/backing-image-manager v1.6.0-dev-20231217.0.20240103150452-7f8aea1edd03
	github.com/longhorn/backupstore v0.0.0-20240110081942-bd231cfb0c7b
	github.com/longhorn/go-common-libs v0.0.0-20240109042507-23627e6416b7
	github.com/longhorn/go-iscsi-helper v0.0.0-20240103085736-72aee873888a
	github.com/longhorn/go-spdk-helper v0.0.0-20240117135122-26f8acb2a13d
	github.com/longhorn/longhorn-engine v1.6.0-dev-20240105.0.20240110095344-deb8b18a1558
	github.com/longhorn/longhorn-instance-manager v1.6.0-dev-20240105.0.20240126085307-467528c95307
	github.com/longhorn/longhorn-share-manager v1.6.0-dev-20231217.0.20231226052309-99d57c1695ea
>>>>>>> 181c414a (Support proxy connections over TLS)
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/rancher/dynamiclistener v0.3.1
	github.com/rancher/go-rancher v0.1.1-0.20220412083059-ff12399dd57b
	github.com/rancher/wrangler v0.8.11
	github.com/robfig/cron v1.2.0
<<<<<<< HEAD
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	github.com/urfave/cli v1.22.5
	golang.org/x/mod v0.8.0
	golang.org/x/net v0.17.0
	golang.org/x/sys v0.13.0
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/grpc v1.40.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
=======
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.8.4
	github.com/urfave/cli v1.22.13
	golang.org/x/mod v0.13.0
	golang.org/x/net v0.20.0
	golang.org/x/sys v0.16.0
	golang.org/x/time v0.3.0
	google.golang.org/grpc v1.60.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
>>>>>>> 181c414a (Support proxy connections over TLS)
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.23.6
	k8s.io/apiextensions-apiserver v0.18.0
	k8s.io/apimachinery v0.27.1
	k8s.io/cli-runtime v0.23.6
	k8s.io/client-go v0.23.6
	k8s.io/kubernetes v1.23.6
	k8s.io/metrics v0.23.6
	k8s.io/mount-utils v0.27.1
	k8s.io/utils v0.0.0-20230406110748-d93618cff8a2
	sigs.k8s.io/controller-runtime v0.4.0
	sigs.k8s.io/yaml v1.2.0
)

require (
<<<<<<< HEAD
	github.com/bits-and-blooms/bitset v1.2.0 // indirect
	github.com/google/fscrypt v0.3.4 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/opencontainers/selinux v1.8.2 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
)

require (
	github.com/Microsoft/go-winio v0.4.17 // indirect
	github.com/RoaringBitmap/roaring v0.4.18 // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/cyphar/filepath-securejoin v0.2.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
=======
	github.com/antlr/antlr4/runtime/Go/antlr/v4 v4.0.0-20230305170008-8188dc5388df // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/google/pprof v0.0.0-20230817174616-7a8ec2ada47b // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/shirou/gopsutil/v3 v3.23.7 // indirect
	github.com/yusufpapurcu/wmi v1.2.3 // indirect
	go.etcd.io/bbolt v1.3.8 // indirect
	golang.org/x/exp v0.0.0-20230725012225-302865e7556b // indirect
	golang.org/x/tools v0.14.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20231212172506-995d672761c0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240108191215-35c7eff3a6b1 // indirect
)

require (
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/RoaringBitmap/roaring v1.2.3 // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.7.0 // indirect
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
>>>>>>> 181c414a (Support proxy connections over TLS)
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/go-co-op/gocron v1.18.0
	github.com/go-logr/logr v1.2.4 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gorilla/context v1.1.1 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/imdario/mergo v0.3.8 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
<<<<<<< HEAD
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
=======
	github.com/longhorn/longhorn-spdk-engine v0.0.0-20240123044045-c5f14845bd83 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
>>>>>>> 181c414a (Support proxy connections over TLS)
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
<<<<<<< HEAD
	github.com/opencontainers/runc v1.0.2 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.28.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rancher/lasso v0.0.0-20211217013041-3c6118a30611 // indirect
=======
	github.com/opencontainers/selinux v1.10.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.4.1-0.20230718164431-9a2bf3000d16 // indirect
	github.com/prometheus/common v0.44.0 // indirect
	github.com/prometheus/procfs v0.11.1 // indirect
	github.com/rancher/lasso v0.0.0-20230830164424-d684fdeb6f29
>>>>>>> 181c414a (Support proxy connections over TLS)
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
<<<<<<< HEAD
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/term v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210831024726-fe130286e0e2 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
=======
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
	golang.org/x/crypto v0.18.0 // indirect
	golang.org/x/oauth2 v0.13.0 // indirect
	golang.org/x/sync v0.4.0
	golang.org/x/term v0.16.0 // indirect
	golang.org/x/text v0.14.0
	google.golang.org/appengine v1.6.8 // indirect
	google.golang.org/genproto v0.0.0-20240102182953-50ed04b92917 // indirect
	google.golang.org/protobuf v1.32.0
>>>>>>> 181c414a (Support proxy connections over TLS)
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiserver v0.23.6 // indirect
	k8s.io/component-base v0.23.6 // indirect
	k8s.io/component-helpers v0.23.6 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.100.1 // indirect
	k8s.io/kube-aggregator v0.18.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
)
