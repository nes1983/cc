#!/bin/bash

# to be executed on the cluster node
# copies the hbase-site and hbase-env files to the conf directory
# only restarts the hbase-master and hbase-regionserver services

cp -v /tmp/hbase-site.xml /etc/hbase/conf/
cp -v /tmp/hbase-env.sh /etc/hbase/conf/

service hbase-master restart
service hbase-regionserver restart