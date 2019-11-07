#!/bin/bash

set -x

namespace=longhorn-system

clean_crs() {
	crd=$1
	kubectl -n $namespace get $crd --no-headers|cut -f1 -d" "| xargs kubectl -n $namespace patch $crd --type='merge' -p '{"metadata":{"finalizers": null}}'
	kubectl -n $namespace delete $crd --all
}

crd_list_v070=(
	volumes.longhorn.io
	replicas.longhorn.io
	engines.longhorn.io
	instancemanagers.longhorn.io
	engineimages.longhorn.io
	nodes.longhorn.io
	settings.longhorn.io
)

crd_list_v062=(
	volumes.longhorn.rancher.io
	replicas.longhorn.rancher.io
	engines.longhorn.rancher.io
	instancemanagers.longhorn.rancher.io
	engineimages.longhorn.rancher.io
	nodes.longhorn.rancher.io
	settings.longhorn.rancher.io
)

version=$1
case $version in
"v062")
	list=("${crd_list_v062[@]}")
	;;
"v070")
	list=("${crd_list_v070[@]}")
	;;
*)
	echo "invalid version to clean up"
	exit 1
esac

for crd in "${list[@]}"
do
	clean_crs $crd
done

#kubectl -n longhorn-system get instancemanagers.longhorn.rancher.io --no-headers|cut -f1 -d" "| xargs kubectl -n longhorn-system patch instancemanagers.longhorn.rancher.io --type='merge' -p '{"metadata":{"finalizers": null}}'
