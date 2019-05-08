#!/bin/bash

set -e

echo "bash update_version.sh <required new version> <optional old version>"
echo "<required new version> is used to update image"
echo "<optional old version> is used to update charts repo"

if [[ $# -le 0 ]]
then
    echo "need to specify release version"
    exit 1
fi

base="${GOPATH}/src/github.com/rancher"
manager_dir="${base}/longhorn-manager"
values_file="${manager_dir}/chart/latest/values.yaml"
chart_file="${manager_dir}/chart/latest/Chart.yaml"

components=("engineTag\:" "managerTag\:" "uiTag\:")

version=$1
version_num=`echo ${version} | sed -r 's/^.{1}//'`

for c in ${components[@]}
do
    sed -i "s/${c}.*/${c} ${version}/g" $values_file
done

sed -i "s/version\:.*/version\: ${version_num}/g" $chart_file
sed -i "s/appVersion\:.*/appVersion\: ${version}/g" $chart_file

echo "updated image version to ${version}"


if [[ $# -eq 2 ]]
then
    old_version=$2
    charts_dir="${base}/charts/proposed/longhorn"

    if [ -e "${charts_dir}/${old_version}" ]; then
        echo "${charts_dir}/${old_version} exists. will not update charts repo"
        exit 1
    fi

    mv "${charts_dir}/latest" "${charts_dir}/${old_version}"
    cp -a "${manager_dir}/chart/latest" "${charts_dir}/latest"

    echo "updated longhorn version from ${old_version} to ${version} in charts repo"
fi
