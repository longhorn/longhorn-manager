Longhorn Manager [![Build Status](https://drone.rancher.io/api/badges/rancher/longhorn-manager/status.svg)](https://drone.rancher.io/rancher/longhorn-manager)
========

Manager for Longhorn.

## Requirement

1. Ubuntu v16.04
2. Docker v1.13+
3. Make sure `iscsiadm`/`open-iscsi` has been installed on the host.
4. Make sure `nfs-kernel-server` has been installed on the host for testing NFS server.

## Building

`make`

## Running

`./bin/longhorn-manager`

## Experimental Server

It can be run as a single node experimental server.

`make server` will start the server bind-mounted to host port `9500`. Then you can use web browser to take a look at it's API UI, by accessing `http://<host_ip>:9500/v1`

This experimental server will contain necessary components for Docker orchestrator to work, e.g. etcd server for k/v store, nfs server for backupstore. Each of them will be started as a container.

The backupstore URL will show up as: `nfs://xxx.xxx.xxx.xxx:/opt/backupstore` in the console when you starting the server. You can update `backupTarget` accordingly in the `v1/settings/backupTarget`.

`make server-cleanup` will cleanup the servers.

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
