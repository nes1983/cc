#!/bin/bash

# call this script to update the hbase configuration on nodes in the cluster
# just add the hostnames as arguments, and the hbase-site.xml and hbase-env.sh will be stored under /tmp/ on the corresponding host

# ssh-user: store the username in a separate file "sshuser"
USER=$(cat sshuser)

for w in $@; do
	rsync -av "../../hbase-conf/${w}/hbase-site.xml" "$USER@${w}:/tmp/"
	rsync -av "../../hbase-conf/hbase-env.sh" "$USER@${w}:/tmp/"
	rsync -av "node_update_hbase.sh" "$USER@${w}:/tmp/"
done

$(csshX --login=$USER $@)
