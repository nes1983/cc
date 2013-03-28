#!/usr/bin/env jruby

# single threaded solution
require 'fileutils'

def cloneRepos
	while (gets)
		process_line $_
	end
end

def force_empty_folder folder
	if File.directory?(folder)
		FileUtils.rm_rf(folder)
	end
	Dir.mkdir(folder)
end

def process_line line
	humanReadableName,name,type,repo = line.split(/\t/)
	
	begin
		humanReadableName = humanReadableName.to_s
		type = type.to_s
		name = name.gsub(/[^A-Za-z]/, "")
		repo = repo.gsub(/\r/,"").gsub(/\n/, "").split(/ /)[0]
		repoName = repo.slice(/.*\/(.+)/, 1)
		$stderr.puts "Processing #{name}: #{repo}"
		
		folder_name = "/tmp/repos/#{name}"
		force_empty_folder(folder_name)
		
		if type.include? "Git"
			$stderr.puts "Begin cloning..."
			%x(git clone --bare #{repo} #{folder_name})
		elsif type.include? "Subversion"
			$stderr.puts "Begin checkout..."
			tmpDir = "/tmp/svn/#{name}"
			force_empty_folder(tmpDir)
			%x(svn co #{repo} #{tmpDir})
			datestamp = Time.now.strftime("%Y-%m-%d")
			%x(cd #{tmpDir} && \
				git init . && \
				git add . && \
				git commit -m "snapshot on #{datestamp}" && \
				git tag -a "head" -m "head" && \
				git gc --aggressive && \
				mv .git #{folder_name})
			FileUtils.rm_rf(tmpDir)
		end
		$stderr.puts "Finished #{name}"
	rescue Exception => e
		$stderr.puts e.message
		$stderr.puts e.backtrace.inspect
	end
end

if __FILE__ == $0
	cloneRepos
end
