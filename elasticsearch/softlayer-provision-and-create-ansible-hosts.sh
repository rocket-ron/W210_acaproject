#!/bin/bash
slcli -y vs create --datacenter=sjc03 --hostname=elasticm1 --domain=w251.rlc --billing=hourly --key=rcordell --cpu=2 --memory=4096 --disk=100 --network=1000 --os=CENTOS_LATEST_64
sleep 5
slcli -y vs create --datacenter=sjc03 --hostname=elasticdata1 --domain=w251.rlc --billing=hourly --key=rcordell --cpu=2 --memory=4096 --disk=100 --network=1000 --os=CENTOS_LATEST_64
sleep 5
slcli -y vs create --datacenter=sjc03 --hostname=elasticdata2 --domain=w251.rlc --billing=hourly --key=rcordell --cpu=2 --memory=4096 --disk=100 --network=1000 --os=CENTOS_LATEST_64

# Wait for softlayer to issue ips to the servers we just created
sleep 300

# Grab the ip addresses
masterip=`slcli vs list | grep elasticm1 | awk '{print $3}'`
datanode1ip=`slcli vs list | grep elasticdata1 | awk '{print $3}'`
datanode2ip=`slcli vs list | grep elasticdata2 | awk '{print $3}'`

DATANODES=($datanode1ip $datanode2ip)
ALLNODES=($masterip ${DATANODES[@]})

# cloud image specific configuration
user=root

# Generate ansible hosts file ##################################################
hostsfile=sl.hosts

echo "[master]" >> $hostsfile
echo "$masterip host_alias=master" >> $hostsfile
hostnum=1
echo "[datanodes]" >> $hostsfile
for datanodeip in "${DATANODES[@]}"
do
  echo "$datanodeip host_alias=datanode$hostnum"  >> $hostsfile
  let hostnum+=1
done

ansible -i sl.hosts all -u root -m ping

# Generate an /etc/hosts file ##################################################
#moved to ansible
#etchostsfile=etc.hosts
#echo "$masterip benchmaster" > $etchostsfile
#hostnum=1
#for slaveip in "${SLAVES[@]}"
#do
#  echo "$slaveip benchslave$hostnum" >> $etchostsfile
#  let hostnum+=1
#done

# Send update to the /etc/hosts file
#for nodeip in "${ALLNODES[@]}"
#do
#  cat $etchostsfile | ssh $user@$nodeip "cat >> /etc/hosts"
#done

# Set up the hosts to be able to communicate with each other ###################
# Note: this has been moved to the ansible playbook
#keyfilename=`cat /dev/urandom | base64 | head -c 8`
#ssh-keygen -t rsa -q -f temp-$keyfilename.key -N ''
#for nodeip in "${ALLNODES[@]}"
#do
#  ssh $user@$nodeip "mkdir -p ~/.ssh"
#  cat temp-$keyfilename.key | ssh $user@$nodeip "cat > ~/.ssh/id_rsa"
#  ssh $user@$nodeip "chmod 600 ~/.ssh/id_rsa"
#  cat temp-$keyfilename.key.pub | ssh $user@$nodeip "cat >> ~/.ssh/authorized_keys"
#done
#rm temp-$keyfilename.key
#rm temp-$keyfilename.key.pub
