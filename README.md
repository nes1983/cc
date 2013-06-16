# Setup (on a recentish OSX) #

 1. Install the latest XCode with command line tools -- or just the [command line tools](https://medium.com/kr-projects/6e54e8c50dc8)
 2. Install [homebrew](http://mxcl.github.io/homebrew/)
 3. Install protobuf: brew install protobuf
 4. Get parallel, as some scripts depend on it. `brew install parallel`
 5. Run the DataFetchPipeline.rb in scripts/
 6. Look at build.xml or the pipeline stages available.

# Hadoop commands #
To check what's inside the generated HAR file:

	hadoop fs -ls -R har:///projects/dataset.har
	
# To download the data #

Copy the local scripts across the cluster:
	
	./scripts/deploy_scripts.sh

Run the DataFetchPipeline:
	
	ssh leela ./scripts/ohloh/DataFetchPipeline.rb

Or locally, for testing.:

	./scripts/ohloh/DataFetchPipeline.rb --max_repos 3