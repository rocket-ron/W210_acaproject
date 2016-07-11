#!/bin/bash
# First remove the ips from our known hosts in case we get these again in the future
masterip=`slcli vs list | grep elasticm1 | awk '{print $3}'`
datanode1ip=`slcli vs list | grep elasticdata1 | awk '{print $3}'`
datanode2ip=`slcli vs list | grep elasticdata2 | awk '{print $3}'`

# Then cancel the vms
masterid=`slcli vs list | grep elasticm1 | awk '{print $1}'`
datanode1id=`slcli vs list | grep elasticdata1 | awk '{print $1}'`
datanode2id=`slcli vs list | grep elasticdata2 | awk '{print $1}'`

ssh-keygen -f "/Users/rcordell/.ssh/known_hosts" -R $masterid
ssh-keygen -f "/Users/rcordell/.ssh/known_hosts" -R $datanode1id
ssh-keygen -f "/Users/rcordell/.ssh/known_hosts" -R $datanode2id


slcli -y vs cancel $masterid
slcli -y vs cancel $datanode1id
slcli -y vs cancel $datanode2id

rm sl.hosts

