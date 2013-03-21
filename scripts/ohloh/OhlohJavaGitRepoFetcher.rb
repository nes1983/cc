#!/usr/bin/env jruby

require 'rubygems'
require 'nokogiri'
require 'open-uri'

# this script fetches all git repositories of projects with
# "java" (as tag | in title | in project description) from ohloh

PROJECTS_PER_PAGE = 10

$countProjectsWithoutGitRepos = 0
$countProjects = 0

def parseProject projectName, projectUrl
	$stderr.puts "Analyze Repo #{projectName} ..."
	
	pageNumber = 1
	
	doc = getDoc(projectUrl, pageNumber)
	
	if (doc.nil?)
		$stderr.puts "doc is nil: #{projectName}, #{projectUrl}"
		return
	end
	
	title = projectUrl.strip.gsub(/\/p\//,"")
	nbGitRepos = doc.css("div.span4").text.strip.slice(/of (\d+)/, 1).to_i
	
	if (nbGitRepos < 1)
		$countProjectsWithoutGitRepos += 1
		return
	end
	
	links = doc.css("td.span4")
	
	while (nbGitRepos > 0)
		links = doc.css("td.span4")
		for l in links
			link = "Git" + "\t" + l.text.gsub(/\r/,"").gsub(/\n/,"")
			puts "#{projectName}\t#{title}\t#{link}"
		end
		pageNumber += 1
        doc = getDoc(projectUrl, pageNumber)
		nbGitRepos -= PROJECTS_PER_PAGE
	end
end

def getDoc (project, pageNumber)
	link = "http://www.ohloh.net#{project}/enlistments?query=Git&sort=type&page=#{pageNumber}"
	begin
		doc = Nokogiri::HTML(open(link))
	rescue Exception => msg
		$stderr.puts "Error occurred in getDoc(#{project}, #{pageNumber}): #{msg}"
		doc = nil
	end
	return doc
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
				projectName = p.text.gsub(/\r/,"").gsub(/\n/,"")
				projectUrl = p.child['href'].to_str
				parseProject projectName, projectUrl
				$countProjects += 1
			rescue Exception => msg
				$stderr.puts "Error occurred on page #{i}: #{msg}"
			end
		end
		$stdout.flush
	end
end

if __FILE__ == $0
	scanRepos
	$stderr.puts "#{$countProjectsWithoutGitRepos} of #{$countProjects} projects had no Git repository."
end
