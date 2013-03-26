export HADOOP_HOME_WARN_SUPPRESS=true
export HADOOP_HOME=/usr/lib/hadoop
export HADOOP_PREFIX=/usr/lib/hadoop

export HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec
export HADOOP_CONF_DIR=/etc/hadoop/conf

export HADOOP_COMMON_HOME=/usr/lib/hadoop
export HADOOP_HDFS_HOME=/usr/lib/hadoop-hdfs
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
export YARN_HOME=/usr/lib/hadoop-yarn

export HADOOP_CLASSPATH=/usr/lib/hadoop/lib/*:$HADOOP_CLASSPATH

export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export YARN_NODEMANAGER_OPTS="-XX:+UseParallelOldGC ${YARN_NODEMANAGER_OPTS}"
