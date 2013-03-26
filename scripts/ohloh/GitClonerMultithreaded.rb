#!/usr/bin/env jruby

require "GitCloner"
require "java"

java_import java.util.concurrent.Executors

$thread_pool = Executors.newFixedThreadPool(29)

def clone_repos_parallel
	while (gets)
		$thread_pool.submit do
			process_line $_
		end
	end
end

if __FILE__ == $0
    clone_repos_parallel
end