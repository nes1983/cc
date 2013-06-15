#!/bin/bash

# Copy all of our scripts to all of our machines. 
# This is useful so you can assume their presence when writing 
# parallel commands.

cat scripts/machines | parallel rsync -av --delete scripts/ {}:~/scripts