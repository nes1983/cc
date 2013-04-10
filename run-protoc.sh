#!/bin/bash

# Run from the root of the source tree.
# Requires the protocol buffer compiler (protoc) to be installed.

find src -name '*.proto' -type f | xargs protoc --java_out=src-gen
