#!/usr/bin/env ruby1.9.3 

# Make an index of all pack files. The format is '%s %n' where (in `stat` syntax), %s is the file size
# in bytes, and %n is the filename.

require_relative 'constants'

# parallel works like xargs, but in parallel. Gnu is needed to get cross-platform behavior.
# The `find` part gets all pack files.
%x(find #{REPO_PATH} -type f -name '*.pack' | parallel --gnu stat -c \'%s %n\' {} > #{REPO_PATH}/index)