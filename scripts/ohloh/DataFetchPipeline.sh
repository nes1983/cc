#!/bin/bash
cd "$(dirname "$0")"
./OhlohJavaGitRepoFetcher.rb \
| tee /tmp/logOhlohJavaGitRepoFetcher \
| ./FilterRepositories.rb \
| tee /tmp/logFilterRepositories \
| ./GitClonerMultithreaded.rb \
| tee /tmp/logGitClonerMultithreaded
