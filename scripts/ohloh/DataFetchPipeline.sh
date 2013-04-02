#!/bin/bash
cd "$(dirname "$0")"
./OhlohJavaRepoFetcher.rb \
| tee /tmp/logOhlohJavaRepoFetcher \
| ./FilterRepositories.rb \
| tee /tmp/logFilterRepositories \
| parallel --pipe ./RepoCloner.rb 2> >(tee -a stderr.log >&2)