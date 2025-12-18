module github.com/longhorn/longhorn-manager

go 1.25.3

toolchain go1.25.5

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
	github.com/henrygd/beszel => github.com/longhorn/beszel v0.16.2-0.20251125001235-162c548010d6
	k8s.io/api => k8s.io/api v0.34.3
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.34.3
	k8s.io/apimachinery => k8s.io/apimachinery v0.34.3
	k8s.io/apiserver => k8s.io/apiserver v0.34.3
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.34.3
	k8s.io/client-go => k8s.io/client-go v0.34.3
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.34.3
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.34.3
	k8s.io/code-generator => k8s.io/code-generator v0.34.3
	k8s.io/component-base => k8s.io/component-base v0.34.3
	k8s.io/component-helpers => k8s.io/component-helpers v0.34.3
	k8s.io/controller-manager => k8s.io/controller-manager v0.34.3
	k8s.io/cri-api => k8s.io/cri-api v0.34.3
	k8s.io/cri-client => k8s.io/cri-client v0.34.3
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.34.3
	k8s.io/dynamic-resource-allocation => k8s.io/dynamic-resource-allocation v0.34.3
	k8s.io/endpointslice => k8s.io/endpointslice v0.34.3
	k8s.io/externaljwt => k8s.io/externaljwt v0.34.3
	k8s.io/kms => k8s.io/kms v0.34.3
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.34.3
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.34.3
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.34.3
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.34.3
	k8s.io/kubectl => k8s.io/kubectl v0.34.3
	k8s.io/kubelet => k8s.io/kubelet v0.34.3
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.30.14
	k8s.io/metrics => k8s.io/metrics v0.34.3
	k8s.io/mount-utils => k8s.io/mount-utils v0.34.3
	k8s.io/pod-security-admission => k8s.io/pod-security-admission v0.34.3
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.34.3
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.34.3
	k8s.io/sample-controller => k8s.io/sample-controller v0.34.3
)

require (
	github.com/cockroachdb/errors v1.12.0
	github.com/container-storage-interface/spec v1.12.0
	github.com/docker/go-connections v0.6.0
	github.com/go-co-op/gocron v1.37.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/handlers v1.5.2
	github.com/gorilla/mux v1.8.1
	github.com/gorilla/websocket v1.5.4-0.20250319132907-e064f32e3674
	github.com/henrygd/beszel v0.17.0
	github.com/jinzhu/copier v0.4.0
	github.com/kubernetes-csi/csi-lib-utils v0.23.0
	github.com/longhorn/backing-image-manager v1.9.2
	github.com/longhorn/backupstore v0.0.0-20251213143132-51fdc08d1cd9
	github.com/longhorn/go-common-libs v0.0.0-20251218054725-dbe74d5605de
	github.com/longhorn/go-iscsi-helper v0.0.0-20251213143157-4a906cc17806
	github.com/longhorn/go-spdk-helper v0.2.1-0.20251213143112-c40a36fce272
	github.com/longhorn/longhorn-engine v1.11.0-dev-20251130.0.20251207134944-819147eb34d2
	github.com/longhorn/longhorn-instance-manager v1.11.0-dev-20251130.0.20251209091223-c9c9640f1991
	github.com/longhorn/longhorn-share-manager v1.9.2
	github.com/longhorn/longhorn-spdk-engine v0.0.0-20251211073105-08609c16d3d1
	github.com/prometheus/client_golang v1.23.2
	// dynamiclistener v0.7.1 has nil pointer dereference issues, so temporarily pin to v0.7.0
	github.com/rancher/dynamiclistener v0.7.3
	github.com/rancher/go-rancher v0.1.1-0.20220412083059-ff12399dd57b
	github.com/rancher/wrangler/v3 v3.3.1
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.11.1
	github.com/urfave/cli v1.22.17
	golang.org/x/mod v0.31.0
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.39.0
	golang.org/x/time v0.14.0
	google.golang.org/grpc v1.77.0
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.34.3
	k8s.io/apiextensions-apiserver v0.34.3
	k8s.io/apimachinery v0.34.3
	k8s.io/cli-runtime v0.34.3
	k8s.io/client-go v0.34.3
	k8s.io/kubernetes v1.34.3
	k8s.io/metrics v0.34.3
	k8s.io/mount-utils v0.34.3
	k8s.io/utils v0.0.0-20251002143259-bc988d571ff4
	sigs.k8s.io/controller-runtime v0.22.4
	sigs.k8s.io/structured-merge-diff/v6 v6.3.1
)

require (
	github.com/0xPolygon/polygon-edge v1.3.3 // indirect
	github.com/Azure/go-ansiterm v0.0.0-20230124172434-306776ec8161 // indirect
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/anmitsu/go-shlex v0.0.0-20200514113438-38f4b401e2be // indirect
	github.com/bits-and-blooms/bitset v1.16.0 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/cockroachdb/logtags v0.0.0-20230118201751-21c54148d20b // indirect
	github.com/cockroachdb/redact v1.1.5 // indirect
	github.com/coreos/go-systemd/v22 v22.6.0 // indirect
	github.com/distatus/battery v0.11.0 // indirect
	github.com/distribution/reference v0.6.0 // indirect
	github.com/dolthub/maphash v0.1.0 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/fxamacker/cbor/v2 v2.9.0 // indirect
	github.com/getsentry/sentry-go v0.27.0 // indirect
	github.com/gliderlabs/ssh v0.3.8 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/godbus/dbus/v5 v5.2.0 // indirect
	github.com/google/gnostic-models v0.7.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/klauspost/compress v1.18.1 // indirect
	github.com/longhorn/types v0.0.0-20251207085945-9c40bd62daff // indirect
	github.com/lufia/plan9stats v0.0.0-20251013123823-9fd1530e3ec3 // indirect
	github.com/lxzan/gws v1.8.9 // indirect
	github.com/mitchellh/go-ps v1.0.0 // indirect
	github.com/moby/term v0.5.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shirou/gopsutil/v4 v4.25.10 // indirect
	github.com/tklauser/go-sysconf v0.3.16 // indirect
	github.com/tklauser/numcpus v0.11.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.yaml.in/yaml/v2 v2.4.2 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20251209150349-8475f28825e9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251022142026-3a174f9686a8 // indirect
	gopkg.in/evanphx/json-patch.v4 v4.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.1 // indirect
	sigs.k8s.io/randfill v1.0.0 // indirect
)

require (
	github.com/Microsoft/go-winio v0.6.2 // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/c9s/goprocinfo v0.0.0-20210130143923-c95fcf8c64a8 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.7 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/emicklei/go-restful/v3 v3.12.2 // indirect
	github.com/evanphx/json-patch v5.9.11+incompatible // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/gammazero/deque v1.0.0 // indirect
	github.com/gammazero/workerpool v1.1.3 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-openapi/jsonpointer v0.21.0 // indirect
	github.com/go-openapi/jsonreference v0.21.0 // indirect
	github.com/go-openapi/swag v0.23.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gorilla/context v1.1.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/mailru/easyjson v0.9.0 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.66.1 // indirect
	github.com/prometheus/procfs v0.16.1 // indirect
	github.com/rancher/lasso v0.2.5 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/slok/goresilience v0.2.0 // indirect
	github.com/spf13/pflag v1.0.10 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/trace v1.38.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0
	golang.org/x/crypto v0.44.0 // indirect
	golang.org/x/oauth2 v0.33.0 // indirect
	golang.org/x/sync v0.19.0
	golang.org/x/term v0.37.0 // indirect
	golang.org/x/text v0.32.0
	google.golang.org/protobuf v1.36.11
	gopkg.in/inf.v0 v0.9.1 // indirect
	k8s.io/apiserver v0.34.3 // indirect
	k8s.io/component-base v0.34.3 // indirect
	k8s.io/component-helpers v0.33.0 // indirect
	k8s.io/controller-manager v0.33.0 // indirect
	k8s.io/klog/v2 v2.130.1 // indirect
	k8s.io/kube-aggregator v0.34.1 // indirect
	k8s.io/kube-openapi v0.0.0-20250710124328-f3f2b991d03b // indirect
	k8s.io/kubelet v0.0.0 // indirect
	sigs.k8s.io/json v0.0.0-20241014173422-cfa47c3a1cc8 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
)
