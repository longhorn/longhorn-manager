#!/bin/bash

VENDOR=rancher.io
DRIVER=longhorn

RETRY_COUNTS=20
RETRY_INTERVAL=1

/checkdependency.sh
if [ $? -ne 0 ]; then
    echo Failed dependency check
    exit -1
fi

# Assuming the single driver file is located at /$DRIVER inside the DaemonSet image.

echo Detecting backend service IP for $LONGHORN_BACKEND_SVC
LONGHORN_BACKEND_SVC_IP=""

found=false
for i in `seq 1 ${RETRY_COUNTS}`;
do
    LONGHORN_BACKEND_SVC_IP=`nslookup ${LONGHORN_BACKEND_SVC} | grep Address: | awk '{if(NR > 1) print $2}'`
    echo ${LONGHORN_BACKEND_SVC_IP} | grep -E "^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$" > /dev/null
    if [ $? -eq 0 ]
    then
	found=true
	break
    fi
    echo Cannot detect valid service IP, retrying
    sleep ${RETRY_INTERVAL}
done

if [ "$found" == false ]
then
    echo Fail to detect valid service IP. Aborting
    exit -1
fi

echo Backend service IP for $LONGHORN_BACKEND_SVC is $LONGHORN_BACKEND_SVC_IP

sed -ri "s|^LONGHORN_SVC=.*|LONGHORN_SVC=\"${LONGHORN_BACKEND_SVC_IP}:9500\"|" "/${DRIVER}"
sed -ri "s|^LONGHORN_NODEID=.*|LONGHORN_NODEID=\"${NODE_NAME}\"|" "/${DRIVER}"

driver_dir=$VENDOR${VENDOR:+"~"}${DRIVER}
if [ ! -d "/flexmnt/$driver_dir" ]; then
  mkdir "/flexmnt/$driver_dir"
fi

cp /jq /binmnt/

cp "/$DRIVER" "/flexmnt/$driver_dir/.$DRIVER"
mv -f "/flexmnt/$driver_dir/.$DRIVER" "/flexmnt/$driver_dir/$DRIVER"

trap "rm -f /flexmnt/$driver_dir/$DRIVER" EXIT

echo Flexvolume driver installed

while : ; do
  sleep 3600
done
