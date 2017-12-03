#!/bin/bash

VENDOR=rancher.io
DRIVER=longhorn

# Assuming the single driver file is located at /$DRIVER inside the DaemonSet image.

LONGHORN_BACKEND_SVC_IP=""
until echo ${LONGHORN_BACKEND_SVC_IP} | grep -E "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$" > /dev/null; do
    LONGHORN_BACKEND_SVC_IP=`nslookup ${LONGHORN_BACKEND_SVC} | grep Address: | awk '{if(NR > 1) print $2}'`
    sleep 1
done

sed -ri "s|^LONGHORN_SVC=.*|LONGHORN_SVC=\"${LONGHORN_BACKEND_SVC_IP}:9500\"|" "/${DRIVER}"
sed -ri "s|^LONGHORN_NODEID=.*|LONGHORN_NODEID=\"${NODE_NAME}\"|" "/${DRIVER}"

driver_dir=$VENDOR${VENDOR:+"~"}${DRIVER}
if [ ! -d "/flexmnt/$driver_dir" ]; then
  mkdir "/flexmnt/$driver_dir"
fi

cp "/$DRIVER" "/flexmnt/$driver_dir/.$DRIVER"
mv -f "/flexmnt/$driver_dir/.$DRIVER" "/flexmnt/$driver_dir/$DRIVER"

while : ; do
  sleep 3600
done

