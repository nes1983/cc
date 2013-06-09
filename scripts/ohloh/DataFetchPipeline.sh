#!/bin/bash

# folder structure inside har file:
# har:///haddock/projects/dataset.har/repos/junit

LOCAL_FOLDER_BASE=/tmp/
# set LOCAL_FOLDER_NAME to the same value as in RepoCloner.rb
LOCAL_FOLDER_NAME=repos
HDFS_TEMP_FOLDER=/tmp/datafetchpipeline/

cd "$(dirname "$0")"
./OhlohJavaRepoFetcher.rb \
| tee /tmp/logOhlohJavaRepoFetcher \
| ./FilterRepositories.rb \
| tee /tmp/logFilterRepositories \
| parallel --pipe ./RepoCloner.rb --repo_path $LOCAL_FOLDER_BASE$LOCAL_FOLDER_NAME 2> >(tee -a stderr.log >&2)

echo "Download phase finished, now copying to HDFS..."
hadoop fs -rm -r -f $HDFS_TEMP_FOLDER
hadoop fs -mkdir $HDFS_TEMP_FOLDER
hadoop fs -copyFromLocal $LOCAL_FOLDER_BASE$LOCAL_FOLDER_NAME $HDFS_TEMP_FOLDER

echo "Creating HAR file..."
hadoop fs -rm -r -f /projects/dataset.har
hadoop archive -archiveName dataset.har -p $HDFS_TEMP_FOLDER $LOCAL_FOLDER_NAME /projects/

echo "Deleting temporary repository folder..."
hadoop fs -rm -r -f $HDFS_TEMP_FOLDER

echo "Finished."