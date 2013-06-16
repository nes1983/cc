#!/usr/bin/env jruby-1.7.4 

# For small experiments, run as ./OhlohJavaRepoFetcher.rb --max_repos 20

require 'rubygems'
require 'nokogiri'
require 'open-uri'
require 'optparse'

# this script fetches all git repositories of projects with
# "java" (as tag | in title | in project description) from ohloh

PROJECTS_PER_PAGE = 10

$countProjectsWithoutGitRepos = 0
$countProjects = 0

def parseProject projectUrl
	$stderr.puts "Analyze Repo #{projectUrl} ..."
	
	pageNumber = 1
	
	doc = getDoc(projectUrl, pageNumber)
	
	if (doc.nil?)
		raise "Could not load project #{projectUrl}"
	end
	
	title = projectUrl.strip.gsub(/\/p\//,"")
	numRepos = doc.css("div.span4").text.strip.slice(/of (\d+)/, 1).to_i
	remaining = numRepos
	
	if (remaining < 1)
		$countProjectsWithoutGitRepos += 1
		return
	end
	
	links = doc.css("td.span4")
	
	while (remaining > 0)
		enlistment = doc.css(".enlistment")
		for e in enlistment
			type = e.css("td:not(.status)").css(".span2").text.gsub(/\r/,"").gsub(/\n/,"")
			link = e.css("td.span4").text.gsub(/\r/,"").gsub(/\n/,"")
			puts "#{title}\t#{type}\t#{link}"
		end
		
		begin
			pageNumber += 1
        	doc = getDoc(projectUrl, pageNumber)
		end until doc or pageNumber > numRepos * PROJECTS_PER_PAGE
			
		remaining -= PROJECTS_PER_PAGE
	end
end

def getDoc (project, pageNumber)
	link = "http://www.ohloh.net#{project}/enlistments?sort=type&page=#{pageNumber}"
	begin
		return Nokogiri::HTML(open(link))
	rescue Exception => msg #
		$stderr.puts "Error occurred in getDoc(#{project}, #{pageNumber}): #{msg}"
		return nil
	end
end

def scanRepos
	start_page_nr = 1
	max_page_nr = Nokogiri::HTML(open("http://www.ohloh.net/p?query=java")).
		text.strip.slice(/\bShowing page 1 of \b([\d,]*)/, 1).gsub(/,/,"").to_i
	for i in (start_page_nr..max_page_nr)
		$stderr.puts "Analyze page #{i}/#{max_page_nr} ..."
		doc = Nokogiri::HTML(open("http://www.ohloh.net/p?query=java&page=#{i}"))
		projects = doc.css("div.project>h2")
		for p in projects
			begin
				projectUrl = p.child['href'].to_str
				parseProject projectUrl
				$countProjects += 1
				if $max_repos > 0 &&  $countProjects>= $max_repos
					return
				end
			rescue Exception => msg
				$stderr.puts "Error occurred on page #{i}: #{msg}"
			end
		end
		$stdout.flush
	end
end

if __FILE__ == $0
	OptionParser.new do |opts|
	$max_repos = -1
	opts.on("--max_repos NUM",
		"How many repos do you want? Choose -1 for all.") do |num|
			$max_repos = num.to_i
		end
	end.parse!
	
	scanRepos
	$stderr.puts "#{$countProjectsWithoutGitRepos} of #{$countProjects} projects had no Git repository."
end
