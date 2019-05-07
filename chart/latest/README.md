# Rancher Longhorn Chart

The following document pertains to running Longhorn from the Rancher 2.0 chart.

## Source Code

Longhorn is 100% open source software. Project source code is spread across a number of repos:

1. Longhorn Engine -- Core controller/replica logic https://github.com/rancher/longhorn-engine
2. Longhorn Manager -- Longhorn orchestration, includes Flexvolume driver for Kubernetes https://github.com/rancher/longhorn-manager
3. Longhorn UI -- Dashboard https://github.com/rancher/longhorn-ui

## Prerequisites

1. Rancher v2.1+
2. Docker v1.13+
3. Kubernetes v1.8+ cluster with 1 or more nodes and Mount Propagation feature enabled. If your Kubernetes cluster was provisioned by Rancher v2.0.7+ or later, MountPropagation feature is enabled by default. [Check your Kubernetes environment now](https://github.com/rancher/longhorn#environment-check-script). If MountPropagation is disabled, the Kubernetes Flexvolume driver will be deployed instead of the default CSI driver. Base Image feature will also be disabled if MountPropagation is disabled.
4. Make sure `curl`, `findmnt`, `grep`, `awk` and `blkid` has been installed in all nodes of the Kubernetes cluster.
5. Make sure `open-iscsi` has been installed in all nodes of the Kubernetes cluster. For GKE, recommended Ubuntu as guest OS image since it contains `open-iscsi` already.

## Uninstallation

1. To prevent damage to the Kubernetes cluster, we recommend deleting all Kubernetes workloads using Longhorn volumes (PersistentVolume, PersistentVolumeClaim, StorageClass, Deployment, StatefulSet, DaemonSet, etc).

2. From Rancher UI, navigate to `Catalog Apps` tab and delete Longhorn app.

## Troubleshooting

### I deleted the Longhorn App from Rancher UI instead of following the uninstallation procedure

Redeploy the (same version) Longhorn App. Follow the uninstallation procedure above.

### Problems with CRDs

If your CRD instances or the CRDs themselves can't be deleted for whatever reason, run the commands below to clean up. Caution: this will wipe all Longhorn state!

```
# Delete CRD finalizers, instances and definitions
for crd in $(kubectl get crd -o jsonpath={.items[*].metadata.name} | tr ' ' '\n' | grep longhorn.rancher.io); do
  kubectl -n ${NAMESPACE} get $crd -o yaml | sed "s/\- longhorn.rancher.io//g" | kubectl apply -f -
  kubectl -n ${NAMESPACE} delete $crd --all
  kubectl delete crd/$crd
done
```

### Volume can be attached/detached from UI, but Kubernetes Pod/StatefulSet etc cannot use it

Check if volume plugin directory has been set correctly. This is automatically detected unless user explicitly set it.

By default, Kubernetes uses `/usr/libexec/kubernetes/kubelet-plugins/volume/exec/`, as stated in the [official document](https://github.com/kubernetes/community/blob/master/contributors/devel/flexvolume.md#prerequisites).

Some vendors choose to change the directory for various reasons. For example, GKE uses `/home/kubernetes/flexvolume` instead.

User can find the correct directory by running `ps aux|grep kubelet` on the host and check the `--volume-plugin-dir` parameter. If there is none, the default `/usr/libexec/kubernetes/kubelet-plugins/volume/exec/` will be used.

---
Please see [link](https://github.com/rancher/longhorn) for more information.
