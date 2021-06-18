Longhorn Manager
========
[![Build Status](https://drone-publish.longhorn.io/api/badges/longhorn/longhorn-manager/status.svg)](https://drone-publish.longhorn.io/longhorn/longhorn-manager)[![Go Report Card](https://goreportcard.com/badge/github.com/rancher/longhorn-manager)](https://goreportcard.com/report/github.com/rancher/longhorn-manager)

Manager for Longhorn.

## Requirement

1. Existing Kubernetes Cluster 1.8+
2. Make sure `iscsiadm`/`open-iscsi` has been installed on the host.
3. Make sure `jq`, `findmnt`, `curl` has been installed on the host, for the Longhorn Flexvolume Driver.

## Build

`make`

## Deployment

`kubectl create -Rf deploy/install`

It will deploy the following components in the `longhorn-system` namespace:
1. Longhorn Manager
2. Longhorn Flexvolume Driver for Kubernetes
3. Longhorn UI

## Cleanup

Longhorn CRD has finalizers in them, so user should delete the volumes and related resource first, give manager a chance to clean up after them.

To prevent damage to the Kubernetes cluster, we recommend deleting all Kubernetes workloads using Longhorn volumes (PersistentVolume, PersistentVolumeClaim, StorageClass, Deployment, StatefulSet, DaemonSet, etc).

1. Create the uninstallation job to cleanly purge CRDs from the system and wait for success:
  ```
  kubectl create -f deploy/uninstall/uninstall.yaml
  kubectl get job/longhorn-uninstall -w
  ```

Example output:
```
$ kubectl create -f https://raw.githubusercontent.com/rancher/longhorn/master/uninstall/uninstall.yaml
serviceaccount/longhorn-uninstall-service-account created
clusterrole.rbac.authorization.k8s.io/longhorn-uninstall-role created
clusterrolebinding.rbac.authorization.k8s.io/longhorn-uninstall-bind created
job.batch/longhorn-uninstall created

$ kubectl get job/longhorn-uninstall -w
NAME                 COMPLETIONS   DURATION   AGE
longhorn-uninstall   0/1           3s         3s
longhorn-uninstall   1/1           20s        20s
^C
```

2. Remove remaining components:
  ```
  kubectl delete -Rf deploy/install
  kubectl delete -f deploy/uninstall/uninstall.yaml
  ```

Tip: If you try `kubectl delete -Rf deploy/install` first and get stuck there, 
pressing `Ctrl C` then running `kubectl create -f deploy/uninstall/uninstall.yaml` can also help you remove Longhorn. Finally, don't forget to cleanup remaining components by running `kubectl delete -f deploy/uninstall/uninstall.yaml`.


## Integration test

See [longhorn-tests repo](https://github.com/rancher/longhorn-tests/tree/master/manager/integration)

## Notes:

### Google Kubernetes Engine
You will need to create cluster-admin role binding for yourselves before creating the deployment, see
[here](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control) for details.
```
kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=<name@example.com>
```

### Flexvolume Plugin Directory
By default we're using the [default Flexvolume Plugin directory](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-storage/flexvolume.md#prerequisitess), which is `/usr/libexec/kubernetes/kubelet-plugins/volume/exec/`.

For GKE 1.8+, it should be at: `/home/kubernetes/flexvolume`.

You may need to change `deploy/deploy.yaml` volume `flexvolume-longhorn-mount` location according to your own environment.

## Contribution

Please check [the Longhorn repo](https://github.com/longhorn/longhorn#community) for the contributing guide.

## License
Copyright (c) 2014-2019 The Longhorn Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
