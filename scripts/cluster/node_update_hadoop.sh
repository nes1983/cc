#!/bin/bash

# to be executed on the cluster node
# copies the core-site and hadoop-env files to the conf directory
# only restarts the hadoop-yarn-nodemanager and hadoop-yarn-resourcemanager services

cp -v /tmp/core-site.xml /etc/hadoop/conf/
cp -v /tmp/hadoop-env.sh /etc/hadoop/conf/

service hadoop-yarn-nodemanager restart
service hadoop-yarn-resourcemanager restart