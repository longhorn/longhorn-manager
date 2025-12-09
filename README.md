Longhorn Manager
========
[![Build Status](https://github.com/longhorn/longhorn-manager/actions/workflows/build.yml/badge.svg)](https://github.com/longhorn/longhorn-manager/actions/workflows/build.yml)[![Go Report Card](https://goreportcard.com/badge/github.com/rancher/longhorn-manager)](https://goreportcard.com/report/github.com/rancher/longhorn-manager)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flonghorn%2Flonghorn-manager.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Flonghorn%2Flonghorn-manager?ref=badge_shield)

Manager for Longhorn.

## Requirements

- Kubernetes cluster:
  - Kubernetes v1.21 or later is installed.
  - [Mount propagation](https://kubernetes-csi.github.io/docs/deploying.html#enabling-mount-propagation) is enabled.
- Host:
  - `iscsiadm`/`open-iscsi` and `nfs-common`/`nfs-utils`/`nfs-client` are installed.
  - The filesystem (ext4 or XFS) supports the `file extents` feature for data storage.

Run the [environment_check.sh](https://raw.githubusercontent.com/longhorn/longhorn/master/scripts/environment_check.sh) script to check if your system meets the listed requirements. For more information, see the [Longhorn documentation](https://longhorn.io/docs/latest/deploy/install/#installation-requirements).

## Build

`make`

## Deployment

- `kubectl create -f https://raw.githubusercontent.com/longhorn/longhorn/master/deploy/longhorn.yaml`

It will deploy the following components in the `longhorn-system` namespace:

- Longhorn Manager
- Longhorn Instance Manager
- Longhorn CSI plugin components
- Longhorn UI

For more information, see the [Longhorn documentation](https://longhorn.io/docs/latest/deploy/install/).

## Cleanup

Longhorn CRD has finalizers in them, so user should delete the volumes and related resource first, give manager a chance to clean up after them.

To prevent damage to the Kubernetes cluster, we recommend deleting all Kubernetes workloads using Longhorn volumes (PersistentVolume, PersistentVolumeClaim, StorageClass, Deployment, StatefulSet, DaemonSet, etc).

1. Ensure that the value of the Longhorn setting `deleting-confirmation-flag` is `true`.

```bash
kubectl -n longhorn-system edit setting deleting-confirmation-flag
```

2. Create the uninstallation job to cleanly purge CRDs from the system. Verify that uninstallation was successful.

```bash
kubectl create -f https://raw.githubusercontent.com/longhorn/longhorn/master/uninstall/uninstall.yaml
kubectl get job/longhorn-uninstall -w
```

Example output:

```bash
$ kubectl create -f https://raw.githubusercontent.com/longhorn/longhorn/master/uninstall/uninstall.yaml
serviceaccount/longhorn-uninstall-service-account created
clusterrole.rbac.authorization.k8s.io/longhorn-uninstall-role created
clusterrolebinding.rbac.authorization.k8s.io/longhorn-uninstall-bind created
job.batch/longhorn-uninstall created

$ kubectl -n longhorn-system get job/longhorn-uninstall -w
NAME                 COMPLETIONS   DURATION   AGE
longhorn-uninstall   0/1           3s         3s
longhorn-uninstall   1/1           20s        20s
^C
```

3. Remove remaining components.

```bash
kubectl delete -f https://raw.githubusercontent.com/longhorn/longhorn/master/uninstall/uninstall.yaml
kubectl delete -f https://raw.githubusercontent.com/longhorn/longhorn/master/deploy/longhorn.yaml

```

Tip: If you try `kubectl delete -Rf deploy/install` first and get stuck there, pressing `Ctrl C` then running `kubectl create -f deploy/uninstall/uninstall.yaml` can also help you remove Longhorn. Finally, don't forget to cleanup remaining components by running `kubectl delete -f deploy/uninstall/uninstall.yaml`.

## Unit Test

To execute all unit tests, make sure there are no uncommitted changes and run:

```bash
make test
```

If there are uncommitted changes, only the affected modules will be tested.
To execute specific unit tests or all tests matching a regex, run:

```bash
TESTS=="NodeControllerSuite.*" make test
```

## Integration test

See [longhorn-tests repo](https://github.com/rancher/longhorn-tests/tree/master/manager/integration)

## Contribution

Please check [the Longhorn repo](https://github.com/longhorn/longhorn#community) for the contributing guide.

## License

Copyright (c) 2014-2025 The Longhorn Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Flonghorn%2Flonghorn-manager.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Flonghorn%2Flonghorn-manager?ref=badge_large)