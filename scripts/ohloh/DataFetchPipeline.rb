#!/usr/bin/env jruby

# Use this to download data and dump into har.
# To create a smaller sample, run
# ./DataFetchPipeline.rb --max_repos 4

require './constants.rb'

require 'fileutils'
require 'optparse'

ENV["SHELL"] = "/bin/bash"

OptionParser.new do |opts|
$max_repos = -1
opts.on("--max_repos NUM",
	"How many repos do you want? Choose -1 for all.") do |num|
		$max_repos = num.to_i
	end
end.parse!

Dir.chdir(File.dirname(__FILE__))

FileUtils.remove_entry_secure(REPO_PATH, true)
FileUtils.mkdir_p(REPO_PATH)

puts %x{./OhlohJavaRepoFetcher.rb --max_repos #{$max_repos} \
| tee /tmp/fetcher.log \
| ./FilterRepositories.rb \
| tee /tmp/filter.log \
| parallel ./RepoCloner.rb 2>&1  \
| tee /tmp/cloner.log }

die


puts "Download phase finished, now copying to HDFS..."
%x(hadoop fs -rm -r -f #{HDFS_TEMP_FOLDER})
%x(hadoop fs -mkdir #{HDFS_TEMP_FOLDER})
%x(hadoop fs -copyFromLocal #{LOCAL_FOLDER_BASE}#{LOCAL_FOLDER_NAME} #{HDFS_TEMP_FOLDER})

puts "Creating HAR file..."
%x(hadoop fs -rm -r -f /projects/dataset.har)
%x(hadoop archive -archiveName dataset.har -p #{HDFS_TEMP_FOLDER} #{LOCAL_FOLDER_NAME} /projects/)

puts "Deleting temporary repository folder..."
%x(hadoop fs -rm -r -f #{HDFS_TEMP_FOLDER})

puts "Finished."
