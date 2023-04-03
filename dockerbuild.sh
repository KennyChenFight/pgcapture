#!/bin/bash

SHA=$(git rev-parse --short HEAD)
#VERSION=$(git describe --tags)
VERSION=debug
PLATFORM=${1:-linux/amd64}

echo "building rueian/pgcapture:$VERSION in $PLATFORM platform"

docker buildx build --platform $PLATFORM --build-arg SHA=$SHA  --build-arg VERSION=$VERSION -t kennychenfight/pgcapture:latest -t kennychenfight/pgcapture:$VERSION .
