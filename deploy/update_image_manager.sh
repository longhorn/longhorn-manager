#!/bin/bash

set -e

base="${GOPATH}/src/github.com/rancher/longhorn-manager"
files=`find ${base}/deploy/ |grep yaml |sort`
project="rancher\/longhorn-manager"

latest=`cat ${base}/bin/latest_image`
echo latest image ${latest}

escaped_latest=${latest//\//\\\/}

for f in $files
do
	sed -i "s/image\:\ ${project}:.*/image\:\ ${escaped_latest}/g" $f
	sed -i "s/-\ ${project}:.*/-\ ${escaped_latest}/g" $f
done
