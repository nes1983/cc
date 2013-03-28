#!/bin/bash
cd "$(dirname "$0")"
./OhlohJavaRepoFetcher.rb \
| tee /tmp/logOhlohJavaRepoFetcher \
| ./FilterRepositories.rb \
| tee /tmp/logFilterRepositories \
| ./GitClonerMultithreaded.rb \
| tee /tmp/logGitClonerMultithreaded
