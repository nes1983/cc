#!/bin/bash

# Call this script to update the Hadoop and HBase configuration on nodes in the cluster.
# Just add the hostnames as arguments. The generated {core|hbase}-site.xml and {hadoop|hbase}-env.sh will be rsync'd to /tmp/ on the corresponding host.

# ssh-user: store the username in a separate file "sshuser"
USER=$(cat sshuser)

for node in $@; do
	mkdir -p "/tmp/${node}/"

	for application in hadoop hbase; do
		site_prefix="core"
		if [ "$application" = "hbase" ]; then
				site_prefix="hbase"
		fi
		./merge_sh_configurations.sh "../../${application}-conf/cluster-env.sh" "../../${application}-conf/${node}/node-env.sh" > "/tmp/${node}/${application}-env.sh"
		./merge_xml_configurations.sh "../../${application}-conf/cluster-properties.xml" "../../${application}-conf/${node}/node-properties.xml" > "/tmp/${node}/${site_prefix}-site.xml"
		rsync -av "/tmp/${node}/${application}-env.sh" "$USER@${node}:/tmp/"
		rsync -av "/tmp/${node}/${site_prefix}-site.xml" "$USER@${node}:/tmp/"
		rsync -av "node_update_${application}.sh" "$USER@${node}:/tmp/"
	done
done

$(csshX --login=$USER $@)
