#!/usr/bin/env ruby1.9.3 

# Download repositories.
# The repositories to download are specified as the input lines, one line for each repository.
# If the repository was previously downloaded, just update it. Avoid downloading it from
# scratch.

# fails with jruby 1.7.3
# works with ruby 1.9.1 / jruby 1.7.4

# example of folder layout after download:
# /tmp/repos/junit/objects/pack/pack-9e57f6b7f2fabacd8fade8fa390ef3f9a13b646b.pack
# /tmp/repos/maven/objects/pack/pack-621f44a9430e5b6303c3580582160a3e53634553.pack

# the index file contains the size (MB) and the path of each pack file (tab-separated).
# example:
# 8	repos/junit/objects/pack/pack-9e57f6b7f2fabacd8fade8fa390ef3f9a13b646b.pack
# 5	repos/maven/objects/pack/pack-621f44a9430e5b6303c3580582160a3e53634553.pack

# Sample invocation: 
# ./RepoCloner.rb 'tomcat Subversion http://svn.apache.org/repos/asf/tomcat/taglibs/standard/trunk/'
# ./RepoCloner.rb 'jetty   Git     git://git.eclipse.org/gitroot/jetty/org.eclipse.jetty.wtp.git master'

require_relative 'constants'

require 'fileutils'
require 'pty'
require 'expect'
require 'tmpdir'

WAIT_TIME = 600

def process_line line
	# it's safe to split on the tab character because tabs are always encoded in an url
	name, type, repo = line.split(/\s+/)
	begin
		name = name.gsub(/[^A-Za-z]/, "")
		repo = repo.gsub(/\r/,"").gsub(/\n/, "").split(/ /)[0]
		repoName = repo.slice(/.*\/(.+)/, 1)
		$stderr.puts "Processing #{name}: #{repo}"
		
		if name.empty?
			$stderr.puts "Empty name on #{repo}"
			return
		end
		
		folder_name = "#{REPO_PATH}/#{name}"
		
		du_out = "0 ."
		if type.include? "Git"
			$stderr.puts %x{git clone --quiet --bare #{repo} #{folder_name}}
			if $?.exitstatus != 0
				$stderr.puts "Updating #{folder_name}"
				%x(cd #{folder_name}; git fetch --all --quiet; git gc --aggressive)
			else
				$stderr.puts "Cloned #{folder_name}"
			end
			du_out = %x(du -m #{folder_name}/objects/pack/*.pack)
		elsif type.include? "Subversion"
			# TODO: Add incremental update of svn repos. For now, just skip.
			if !File.exist?(folder_name)
				Dir.mktmpdir { |tmpDir|
					ok = spawn("svn co --quiet #{repo} #{tmpDir}")
					return unless ok
					
					datestamp = Time.now.strftime("%Y-%m-%d")
					%x(cd #{tmpDir} && \
						git init . && \
						git add . && \
						git commit -m "snapshot on #{datestamp}" && \
						git tag -a "head" -m "head" && \
						git gc --aggressive && \
						mv .git #{folder_name}) # XXX By all rules of logic, this should be ".git/*". However, 
											# The reasonable doesn't work, and this one does.
					du_out = %x(cd #{folder_name} && du -m ./objects/pack/*.pack)
					$stderr.puts "Finished #{name}"
				}
			else 
				$stderr.puts "Skipped #{name}"
			end
		else 
			$stderr.puts "Unknown repo type ", type, " in ", repo
		end
		
		open("#{REPO_PATH}/index",  "at") do
			|f| f.puts du_out.sub(".",  "#{LOCAL_FOLDER_NAME}/#{name}")
		end
	rescue Exception => e
		$stderr.puts e.message
		$stderr.puts e.backtrace.inspect
	end
end

def spawn(cmd)
	$stderr.puts cmd
	begin
		PTY.spawn(cmd) do |reader, writer, pid|
			reader.expect(/username|\bname|permanent/i, WAIT_TIME) { |name|
				if /permanent/ =~ name
					writer.puts("p\n")
					continue
				end
				
				# Ruby offers no API to see if we ran into the timeout,
				# We could measure the time ourselves, 
				# but it's not actually that important.
				# So we just ignore that case.
				if name
					$stderr.puts("Was asked for username/password by #{cmd}")
					Process.kill("TERM", pid)
					return false
				end
			}
			Process.kill('TERM', pid)
		end
	rescue Errno::EIO
		# That is ok. It just means the child died before we could read it.
	end
	return true
end

if __FILE__ == $0
	FileUtils.mkdir_p(REPO_PATH)
	
	ARGV.map { |el| process_line el }
end
