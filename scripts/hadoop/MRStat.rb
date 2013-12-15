#!/usr/bin/env ruby1.9.3

require 'open-uri'
require 'optparse'
require 'json'

# example call: ./MRStat.rb --session 1386964229949 --start 1 --last 22 > stats.csv 
HOST = "leela.unibe.ch"
PORT = 19888

def genUrl(session, job)
	jobnumber = job.to_s.rjust(4, '0')
	return "http://#{HOST}:#{PORT}/ws/v1/history/mapreduce/jobs/job_#{session}_#{jobnumber}"
end

def write(cellvalue)
	print cellvalue.to_s + ";"
end

def parse(url)
	doc = JSON.parse(open(url).read())
	job = doc["job"]

	write job["name"]
	write (job["finishTime"] - job["startTime"]) / 1000
	write job["state"]
	write job["mapstotal"]
	write job["mapscompleted"]
	write job["reducesTotal"]
	write job["uberized"]
	write job["avgMapTime"] / 1000
	write job["avgReduceTime"] / 1000
	write job["avgShuffleTime"] / 1000
	write job["avgMergeTime"] / 1000
	write job["failedReduceAttempts"]
	write job["killedReduceAttempts"]
	write job["successfulReduceAttempts"]
	write job["failedMapAttempts"]
	write job["killedMapAttempts"]
	write job["successfulMapAttempts"]
end

if __FILE__ == $0
	OptionParser.new do |opts|
		$session = 1
		opts.on("--session NUM", "The session number.") do |num|
			$session = num.to_i
		end

		$start = 1
		opts.on("--start NUM", "With which job do you want to start?") do |num|
			$start = num.to_i
		end

		$last = 1
		opts.on("--last NUM", "At which job you want to stop?") do |num|
			$last = num.to_i
		end
	end.parse!
	
	puts "name;duration;state;mapstotal;mapscompleted;reducesTotal;uberized;avgMapTime;avgReduceTime;avgShuffleTime;avgMergeTime;failedReduceAttempts;killedReduceAttempts;successfulReduceAttempts;failedMapAttempts;killedMapAttempts;successfulMapAttempts"
	for i in ($start..$last)
		url = genUrl($session, i)
		parse(url)
		puts
	end
end
