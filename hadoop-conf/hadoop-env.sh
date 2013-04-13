export JAVA_HOME=/usr/lib/jvm/java-7-oracle

#MRv1
export HADOOP_HOME=/usr/lib/hadoop-0.20-mapreduce
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-0.20-mapreduce
export HADOOP_HEAPSIZE=10000
export HADOOP_JOBTRACKER_OPTS="-server -XX:+UseG1GC ${HADOOP_JOBTRACKER_OPTS}"
export HADOOP_TASKTRACKER_OPTS="-server -XX:+UseG1GC ${HADOOP_TASKTRACKER_OPTS}"

#MRv2
#export YARN_NODEMANAGER_OPTS="-XX:+UseG1GC ${YARN_NODEMANAGER_OPTS}"
