#!/bin/bash
# Build script
set -eo pipefail

build_tag=$1
name=$2

docker build -f Dockerfile --label commitHash=$(git rev-parse --short HEAD) -t ${name}:${build_tag} .
echo {\"image_name\" : \"${name}\", \"image_tag\" : \"${build_tag}\"} > metadata.json