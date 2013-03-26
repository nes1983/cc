#!/usr/bin/env jruby

# single threaded solution
def cloneRepos
	while (gets)
		process_line $_
	end
end

def process_line line
	humanReadableName,name,type,repo = line.split(/\t/)
	
	begin
		humanReadableName = humanReadableName.to_s
		name = name.gsub(/[^A-Za-z]/, "")
		repo = repo.gsub(/\r/,"").gsub(/\n/, "").split(/ /)[0]
		repoName = repo.slice(/.*\/(.+)/, 1)
		folder_name = "/tmp/repos/#{name}"

		$stderr.puts "Processing #{repo}"

		if File.directory?("#{folder_name}")
			Dir.rmdir(folder_name)
		end
		Dir.mkdir("#{folder_name}")

		$stderr.puts "Begin cloning..."
		%x(git clone --bare #{repo} #{folder_name})
	rescue Exception => e
		$stderr.puts e.message
		$stderr.puts e.backtrace.inspect
	end
end

if __FILE__ == $0
	cloneRepos
end