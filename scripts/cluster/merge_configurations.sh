#!/bin/bash
#
# merges two files of property settings together and returns them integrated in a xml configuration file
# first argument is the cluster properties file, second argument the node specific properties file

CLUSTER_PROPS=$1
NODE_PROPS=$2

BEGINNING="<?xml version=\"1.0\"?>\n<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n<configuration>"
ENDING="</configuration>"

echo -e $BEGINNING
echo "$(cat $CLUSTER_PROPS $NODE_PROPS)"
echo -e $ENDING