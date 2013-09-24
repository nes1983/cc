#!/bin/bash

# Windows version of protobuf compilation
# Run from the root of the source tree.
# Requires the protocol buffer compiler (protoc.exe) to be in the PATH. See README.MD, windows section.

mkdir -p src-gen
find src -name '*.proto' -type f | xargs protoc --java_out=src-gen
