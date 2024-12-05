module github.com/longhorn/longhorn-manager

go 1.23

toolchain go1.23.3

// Replace directives are required for dependencies in this section because:
// - This module imports k8s.io/kubernetes.
// - The development for all of these dependencies is done at kubernetes/staging and then synced to other repos.
// - The go.mod file for k8s.io/kubernetes imports these dependencies with version v0.0.0 (which does not exist) and \
//   uses its own replace directives to load the appropriate code from kubernetes/staging.
// - Go is not able to find a version v0.0.0 for these dependencies and cannot meaningfully follow replace directives in
//   another go.mod file.
//
// The solution (which is used by all projects that import k8s.io/kubernetes) is to add replace directives for all
// k8s.io dependencies of k8s.io/kubernetes that k8s.io/kubernetes itself replaces in its go.mod file. The replace
// directives should pin the version of each dependency to the version of k8s.io/kubernetes that is imported. For
// example, if we import k8s.io/kubernetes v1.28.5, we should use v0.28.5 of all the replace directives. Depending on
// the portions of k8s.io/kubernetes code this module actually uses, not all of the replace directives may strictly be
// necessary. However, it is better to include all of them for consistency.
replace (
	k8s.io/api => k8s.io/api v0.31.3
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.31.3
	k8s.io/apimachinery => k8s.io/apimachinery v0.31.3
	k8s.io/apiserver => k8s.io/apiserver v0.31.3
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.31.3
	k8s.io/client-go => k8s.io/client-go v0.31.3
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.31.3
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.31.3
	k8s.io/code-generator => k8s.io/code-generator v0.31.3
	k8s.io/component-base => k8s.io/component-base v0.31.3
	k8s.io/component-helpers => k8s.io/component-helpers v0.31.3
	k8s.io/controller-manager => k8s.io/controller-manager v0.31.3
	k8s.io/cri-api => k8s.io/cri-api v0.31.3
	k8s.io/cri-client => k8s.io/cri-client v0.31.3
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.31.3
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.31.3
	k8s.io/endpointslice => k8s.io/endpointslice v0.31.3
	k8s.io/kms => k8s.io/kms v0.31.3
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.31.3
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.31.3
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.31.3
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.31.3
	k8s.io/kubectl => k8s.io/kubectl v0.31.3
	k8s.io/kubelet => k8s.io/kubelet v0.31.3
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.30.7
	k8s.io/metrics => k8s.io/metrics v0.31.3
	k8s.io/mount-utils => k8s.io/mount-utils v0.31.3
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.31.3
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.31.3
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.31.3
	k8s.io/sample-controller => k8s.io/sample-controller v0.31.3
)

require (
	github.com/container-storage-interface/spec v1.11.0
	github.com/docker/go-connections v0.5.0
	github.com/go-co-op/gocron v1.37.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.3
	github.com/jinzhu/copier v0.4.0
	github.com/kubernetes-csi/csi-lib-utils v0.19.0
	github.com/longhorn/backing-image-manager v1.8.0-dev-20241201
	github.com/longhorn/backupstore v0.0.0-20241130163459-2b482603a2c6
	github.com/longhorn/go-common-libs v0.0.0-20241128023039-4d6c3a880dbc
	github.com/longhorn/go-iscsi-helper v0.0.0-20241130163427-b18631536a86
	github.com/longhorn/go-spdk-helper v0.0.0-20241202131855-7d9a097456b2
	github.com/longhorn/longhorn-engine v1.8.0-dev-20241201
	github.com/longhorn/longhorn-instance-manager v1.8.0-dev-20241201
	github.com/longhorn/longhorn-share-manager v1.8.0-dev-20241117.0.20241125222022-2a7cb41cd62a
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.20.5
	github.com/rancher/dynamiclistener v0.6.1
	github.com/rancher/go-rancher v0.1.1-0.20220412083059-ff12399dd57b
	github.com/rancher/wrangler/v3 v3.1.0
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.10.0
	github.com/urfave/cli v1.22.16
	golang.org/x/mod v0.22.0
	golang.org/x/net v0.31.0
	golang.org/x/sys v0.27.0
	golang.org/x/time v0.8.0
	google.golang.org/grpc v1.68.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.31.3
	k8s.io/apiextensions-apiserver v0.31.3
	k8s.io/apimachinery v0.31.3
	k8s.io/cli-runtime v0.31.3
	k8s.io/client-go v1.5.2
	k8s.io/kubernetes v1.31.3
	k8s.io/metrics v0.31.3
	k8s.io/mount-utils v0.31.3
	k8s.io/utils v0.0.0-20241104163129-6fe5fd82f078
	sigs.k8s.io/controller-runtime v0.19.2
)

require (
	github.com/Azure/go-ansiterm v0.0.0-20210617225240-d185dfc1b5a1 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/distribution/reference v0.5.0 // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/gnostic-models v0.6.8 // indirect
	github.com/jonboulle/clockwork v0.4.0 // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/longhorn/types v0.0.0-20241123075624-48c550af4eab // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/moby/sys/userns v0.1.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/opencontainers/runc v1.2.2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/tools v0.27.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241118233622-e639e219e697 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/Microsoft/go-winio v0.6.1 // indirect
	github.com/NYTimes/gziphandler v1.1.1 // indirect
	github.com/asaskevich/govalidator v0.0.0-20190424111038-f61b66f89f4a // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/c9s/goprocinfo v0.0.0-20210130143923-c95fcf8c64a8 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emicklei/go-restful/v3 v3.12.1 // indirect
	github.com/evanphx/json-patch v5.9.0+incompatible // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/gammazero/deque v0.2.1 // indirect
	github.com/gammazero/workerpool v1.1.3 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/google/cel-go v0.20.1 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/gorilla/context v1.1.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.20.0 // indirect
	github.com/imdario/mergo v0.3.13 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rancher/lasso v0.0.0-20241125180327-d19dc78eb0a8
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/slok/goresilience v0.2.0 // indirect
	github.com/spf13/cobra v1.8.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.14 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.14 // indirect
	go.etcd.io/etcd/client/v3 v3.5.14 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.53.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.53.0 // indirect
	go.opentelemetry.io/otel v1.28.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.28.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.27.0 // indirect
	go.opentelemetry.io/otel/metric v1.28.0 // indirect
	go.opentelemetry.io/otel/sdk v1.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.28.0 // indirect
	go.opentelemetry.io/proto/otlp v1.3.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.26.0 // indirect
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/oauth2 v0.24.0 // indirect
	golang.org/x/sync v0.9.0
	golang.org/x/term v0.26.0 // indirect
	golang.org/x/text v0.20.0
	google.golang.org/genproto v0.0.0-20240401170217-c3f982113cda // indirect
	google.golang.org/protobuf v1.35.2
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	k8s.io/apiserver v0.31.3 // indirect
	k8s.io/cloud-provider v0.0.0 // indirect
	k8s.io/component-base v0.31.3 // indirect
	k8s.io/component-helpers v0.31.3 // indirect
	k8s.io/controller-manager v0.31.3 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kms v0.31.3 // indirect
	k8s.io/kube-aggregator v0.31.1 // indirect
	k8s.io/kube-openapi v0.0.0-20241105132330-32ad38e42d3f // indirect
	k8s.io/kubelet v0.0.0 // indirect
	sigs.k8s.io/apiserver-network-proxy/konnectivity-client v0.30.3 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.3 // indirect
	sigs.k8s.io/yaml v1.4.0 // indirect
)

replace github.com/longhorn/types v0.0.0-20241123075624-48c550af4eab => github.com/derekbit/longhorn-types v0.0.0-20241128141620-e298a5a080b7

replace github.com/longhorn/longhorn-instance-manager v1.8.0-dev-20241201 => github.com/derekbit/longhorn-instance-manager v0.0.0-20241204043532-2a568b506202
