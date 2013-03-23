#!/bin/bash

# call this script to update the hadoop configuration on nodes in the cluster
# just add the hostnames as arguments, and the generated core-site.xml will be stored under /tmp/ on the corresponding host

# ssh-user: store the username in a separate file "sshuser"
USER=$(cat sshuser)

for w in $@; do
	./merge_configurations.sh "../../hadoop-conf/cluster-properties.xml" "../../hadoop-conf/${w}/node-properties.xml" > "/tmp/${w}/core-site.xml"
	rsync -av "/tmp/${w}/core-site.xml" "$USER@${w}:/tmp/"
	rm "/tmp/${w}/core-site.xml"
done

$(csshX --login=$USER $@)
