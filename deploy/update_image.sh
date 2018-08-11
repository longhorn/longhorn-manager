#!/bin/bash

set -e

while [[ $# -gt 0 ]]
do
	key="$1"
	case $key in
		-e|--engine)
			engine="$2"
			shift
			shift
			;;
		-u|--ui)
			ui="$2"
			shift
			shift
			;;
		*)
			echo Unknown parameter $1
			exit 1
			;;
	esac
done

base="${GOPATH}/src/github.com/rancher/longhorn-manager"
files=`find ${base}/deploy/ |grep yaml |sort`

project="rancher\/longhorn-manager"
latest=`cat ${base}/bin/latest_image`
echo latest manager image ${latest}

escaped_image=${latest//\//\\\/}

for f in $files
do
	sed -i "s/image\:\ ${project}:.*/image\:\ ${escaped_image}/g" $f
	sed -i "s/-\ ${project}:.*/-\ ${escaped_image}/g" $f
done

if [ -n "$engine" ]; then
	project="rancher\/longhorn-engine"
	echo engine image $engine
	escaped_image=${engine//\//\\\/}

	for f in $files
	do
		sed -i "s/-\ ${project}:.*/-\ ${escaped_image}/g" $f
	done
fi

if [ -n "$ui" ]; then
	project="rancher\/longhorn-ui"
	echo ui image $ui
	escaped_image=${ui//\//\\\/}

	for f in $files
	do
		sed -i "s/image\:\ ${project}:.*/image\:\ ${escaped_image}/g" $f
	done
fi
