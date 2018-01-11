Longhorn Manager [![Build Status](https://drone.rancher.io/api/badges/rancher/longhorn-manager/status.svg)](https://drone.rancher.io/rancher/longhorn-manager)
========

Manager for Longhorn.

## Requirement

1. Existing Kubernetes Cluster 1.8+
2. Make sure `iscsiadm`/`open-iscsi` has been installed on the host.
3. Make sure `jq`, `findmnt`, `curl` has been installed on the host, for the Longhorn Flexvolume Driver.

## Build

`make`

## Deployment

`kubectl create -f deploy/deploy.yaml`

It will deploy:
1. Longhorn Manager
2. Longhorn Flexvolume Driver for Kubernetes
3. Longhorn UI

## Cleanup

`kubectl delete -f deploy/deploy.yaml`

`./deploy/cleanup-crd.sh`

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
By default we're using the GKE Flexvolume Plugin directory, which is at: `/home/kubernetes/flexvolume`.

You may need to change `deploy/deploy.yaml` volume `flexvolume-longhorn-mount` location according to your own environment.

## License
Copyright (c) 2014-2017 [Rancher Labs, Inc.](http://rancher.com)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
