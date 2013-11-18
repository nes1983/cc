Note: we are currently using protobuf 2.4.0a

# Setup (on a recentish OSX) #

 1. Install the latest XCode with command line tools -- or just the [command line tools](https://medium.com/kr-projects/6e54e8c50dc8)
 2. Install [homebrew](http://mxcl.github.io/homebrew/)
 3. Install protobuf: brew install protobuf
 4. Get parallel, as some scripts depend on it. `brew install parallel`
 5. Run the DataFetchPipeline.rb in scripts/
 6. Look at build.xml or the pipeline stages available.

# Setup (on MS Windows) #

 1. Install the Google's protocol buffers binaries: https://protobuf.googlecode.com
 2. Unpack the archive and put protoc.exe somewhere in PATH
 3. Run run-protoc-win.sh to generate protobuf's classes
 Steps 4-6 are the same as for OSX

# Monitoring the cluster #
To check what's inside the generated HAR file:

	hadoop fs -ls -R har:///projects/dataset.har

- [Hadoop Cluster](http://haddock:8088)
- [DFS Health](http://haddock:50070)
- [HBase Master](http://leela:60010)

# Running hadoop tests with junit outside the unibe network #

 1. Make sure that you have a public ssh key. If you don't, follow this guide: https://help.github.com/articles/generating-ssh-keys
 2. Append your public ssh key to `deploy@haddock.unibe.ch ~/.ssh/authorized_keys`
 3. Make sure that you can `ssh haddock.unibe.ch -l deploy` without being asked a password.
 4. Run your test with `./ant.sh uploadJar -DmainClass=ch.unibe.scg.cells.hadoop.JUnitRunner -DclassArgument=ch.unibe.scg.cells.hadoop.HadoopPipelineTest`.
    In case of an unsuccessful run you, you will get the errors in the console.

# To kill a hang job #
	hadoop job -kill job_<your_job_id>

# To download the data #

Copy the local scripts across the cluster:

	./scripts/deploy_scripts.sh

Run the DataFetchPipeline:

	ssh leela ./scripts/ohloh/DataFetchPipeline.rb

Or locally, for testing.:

	./scripts/ohloh/DataFetchPipeline.rb --max_repos 3


# HBase shell #

Open with:

	hbase shell

List tables:

	list


# Check MR ouptput #

Check HBase table size:

	hadoop fs -du -h -s /hbase/

Check size of HAR file:

	hadoop fs -du -h /projects/dataset.har