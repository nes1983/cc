#!/usr/bin/env ruby1.9.3

# Use this to download data and dump into har.
# To create a smaller sample, run
# ./DataFetchPipeline.rb --max_repos 4

require_relative 'constants'

require 'fileutils'
require 'optparse'

$max_repos = -1
$skip_download = false

# Cross-platform way of finding an executable in the $PATH.
#
#   which('ruby') #=> /usr/bin/ruby
def which(cmd)
	# Stolen from http://stackoverflow.com/questions/210872
	exts = ENV['PATHEXT'] ? ENV['PATHEXT'].split(';') : ['']
	ENV['PATH'].split(File::PATH_SEPARATOR).each do |path|
		exts.each { |ext|
			exe = File.join(path, "#{cmd}#{ext}")
			return exe if File.executable? exe
		}
	end
  	return nil
end

def init()
	abort ("You need to have parallel installed") unless which("parallel")
	OptionParser.new do |opts|
		opts.on("--max_repos NUM", "How many repos do you want? Choose -1 for all.") do |num| 
			$max_repos = num.to_i
		end
		opts.on("--skip_download", "Skip the download phase altogether.") do |skip_download|
			$skip_download = skip_download
		end
	end.parse!

	Dir.chdir(File.dirname(__FILE__))
	
	FileUtils.mkdir_p(REPO_PATH)
	FileUtils.remove_entry_secure(LOG_FILE, true)
end

def download()
	puts %x{./OhlohJavaRepoFetcher.rb --max_repos #{$max_repos} 2>>#{LOG_FILE} \
	| tee /tmp/fetched \
	| ./FilterRepositories.rb 2>>#{LOG_FILE} \
	| tee /tmp/filtered \
	| parallel --gnu ./RepoCloner.rb 2>>#{LOG_FILE}}
	# In the above lines, the --gnu switch is very important. parallel will fail silently on Ubuntu unless it is set.
	
	log = File.read(LOG_FILE)
	if log.length > 0
		puts "Obtained Log: ", log
		# We write errors (and regular log information) to LOG_FILE,
		# so we can't just abort here.
	end
	%x(./MakeIndex.rb)
end

def moveToHadoop()
	puts "Download phase finished, now copying to HDFS..."
	puts %x(hadoop fs -rm -r -f #{HDFS_TEMP_FOLDER} 2>&1)
	puts %x(hadoop fs -mkdir #{HDFS_TEMP_FOLDER} 2>&1)
	puts %x(hadoop fs -copyFromLocal #{LOCAL_FOLDER_BASE}#{LOCAL_FOLDER_NAME} \
		#{HDFS_TEMP_FOLDER} 2>&1)
	
	puts "Creating HAR file..."
	puts %x(hadoop fs -rm -r -f /projects/dataset.har 2>&1)
	puts %x(hadoop archive -archiveName dataset.har -p #{HDFS_TEMP_FOLDER} \
		#{LOCAL_FOLDER_NAME} /projects/ 2>&1)
	
	puts "Deleting temporary repository folder..."
	puts %x(hadoop fs -rm -r -f #{HDFS_TEMP_FOLDER} 2>&1)
	
	puts "Finished."
end

if __FILE__ == $0
	init()
	download() unless $skip_download
	moveToHadoop()
end