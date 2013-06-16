#!/usr/bin/env jruby-1.7.4 

# Use this to download data and dump into har.
# To create a smaller sample, run
# ./DataFetchPipeline.rb --max_repos 4

require_relative 'constants'

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
FileUtils.remove_entry_secure(LOG_FILE, true)

puts %x{./OhlohJavaRepoFetcher.rb --max_repos #{$max_repos} 2>>#{LOG_FILE} \
| tee /tmp/fetched \
| ./FilterRepositories.rb 2>>#{LOG_FILE} \
| tee /tmp/filtered \
| parallel ./RepoCloner.rb 2>>#{LOG_FILE}  \
| tee /tmp/cloned }

log = File.read(LOG_FILE)
if log.length > 0
	puts "Obtained Log: ", log
	# We write errors (and regular log information) to LOG_FILE,
	# so we can't just abort here.
end

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
