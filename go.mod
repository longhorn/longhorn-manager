module github.com/longhorn/longhorn-manager

go 1.17

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
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.5.0
	github.com/jinzhu/copier v0.3.5
	github.com/kubernetes-csi/csi-lib-utils v0.6.1
	github.com/longhorn/backing-image-manager v0.0.0-20220609065820-a08f7f47442f
	github.com/longhorn/backupstore v0.0.0-20230322065341-e850db46c6cf
	github.com/longhorn/go-iscsi-helper v0.0.0-20230215054929-acb305e1031b
	github.com/longhorn/longhorn-engine v1.4.0-rc1.0.20230317063250-735a7f5fddc3
	github.com/longhorn/longhorn-instance-manager v1.4.0-rc1.0.20230307055833-15b0fed1ba66
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.1
	github.com/rancher/dynamiclistener v0.3.1
	github.com/rancher/go-rancher v0.1.1-0.20220412083059-ff12399dd57b
	github.com/rancher/wrangler v1.0.0
	github.com/robfig/cron v1.2.0
	github.com/sirupsen/logrus v1.9.0
	github.com/stretchr/testify v1.8.1
	github.com/urfave/cli v1.22.2
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4
	golang.org/x/net v0.7.0
	golang.org/x/sys v0.5.0
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac
	google.golang.org/grpc v1.53.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/api v0.24.0
	k8s.io/apiextensions-apiserver v0.24.0
	k8s.io/apimachinery v0.26.0
	k8s.io/cli-runtime v0.23.6
	k8s.io/client-go v0.24.0
	k8s.io/kubernetes v1.23.6
	k8s.io/metrics v0.23.6
	k8s.io/mount-utils v0.26.0
	k8s.io/utils v0.0.0-20221107191617-1a15be271d1d
	sigs.k8s.io/controller-runtime v0.10.1
	sigs.k8s.io/yaml v1.3.0
)

require (
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/google/fscrypt v0.3.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
)

require (
	github.com/Microsoft/go-winio v0.4.17 // indirect
	github.com/RoaringBitmap/roaring v0.4.18 // indirect
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0 // indirect
	github.com/cyphar/filepath-securejoin v0.2.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/docker/distribution v2.7.1+incompatible // indirect
	github.com/evanphx/json-patch v4.12.0+incompatible // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/go-co-op/gocron v1.18.0
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gorilla/context v1.1.1 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/liggitt/tabwriter v0.0.0-20181228230101-89fcab3d43de // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/runc v1.0.2 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.28.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/rancher/lasso v0.0.0-20211217013041-3c6118a30611 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/oauth2 v0.4.0 // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4 // indirect
	golang.org/x/term v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/apiserver v0.23.6 // indirect
	k8s.io/component-base v0.23.6 // indirect
	k8s.io/component-helpers v0.23.6 // indirect
	k8s.io/klog v1.0.0 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/kube-aggregator v0.24.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
)
