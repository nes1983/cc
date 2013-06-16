#!/usr/bin/env jruby-1.7.4 

# This script filters the repositories found by the OhlohJavaGitRepoFetcher.rb script,
# such that there is always only one Git repository per project. It tries to evaluate the best link.
# The input is expected to be tab separated:
# definition:	name	type	url
# example: Apache Maven 2	maven2	Git	https://git-wip-us.apache.org/repos/asf/maven-scm.git master


# prefer "src" / "source" / "core" / "standard" / "build" / "master" appears in url
# prefer normalized name appearing multiple times (http://subclipse.tigris.org/svn/subclipse/trunk/subclipse)
# prefer short url
# prefer Git repos
# sort descending and choose first result after ranking

def filterRepositories
	Enumerator.new { |repos|
		while (gets)
			repo = Repo.new
			repo.name, repo.type, repo.url = $_.split(/\t/)
			repo.score = 0
			
			repos << repo
		end
	}.group_by{|repo| repo.name}.values.each{|repoList|
		printBest repoList
	}
end

def printBest repos
	repos.each {|repo|
		# prefer "src" / "source" / "core" / "standard" / "build" / "master" appears in url
		if (repo.url =~ /src|source|core|standard|build|master/i)
			repo.score += 75
		end
		
		if (repo.url !~ /doc|docs|extras|extra|tool|tools|test|testing/i)
			repo.score -= 75
		end
				
		# prefer normalized name appearing multiple times (http://subclipse.tigris.org/svn/subclipse/trunk/subclipse)
		name_normalized = repo.name.downcase.gsub(/[^a-z ]/, '').gsub(/ /, '')
		url_normalized = repo.url.downcase.gsub(/[^a-z ]/, '').gsub(/ /, '')
		repo.score += url_normalized.scan(name_normalized).length*10
		
		# prefer short url
		repo.score += (1000 / repo.url.length)
		
		# prefer Git repos
		if (repo.type =~ /git/i)
			repo.score += 100
		end
	}
	
	# sort descending and choose first result after ranking
	bestRepo = repos.max_by{|r| r.score}
	$stdout.puts "#{bestRepo.name}\t#{bestRepo.type}\t#{bestRepo.url}"
end

class Repo
	attr_accessor :name, :type, :url, :score
end

if __FILE__ == $0
	filterRepositories
end