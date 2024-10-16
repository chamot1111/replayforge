#!/bin/bash

if [ -n "$(git status --porcelain)" ]; then
    echo "Git working directory is not clean. Please commit or stash your changes."
    exit 1
fi

current_tag=$(git describe --tags --exact-match 2> /dev/null)
if [ -n "$current_tag" ]; then
    echo "Current tag: $current_tag"
else
    echo "No tag found on the current commit."
fi

rm -rf cmd/player/dist cmd/proxy/dist cmd/relay/dist

if [ -z "$GITHUB_TOKEN" ]; then
    if [ -f ~/.github_token ]; then
        export GITHUB_TOKEN=$(cat ~/.github_token)
    else
        echo "GITHUB_TOKEN not set and ~/.github_token not found"
        exit 1
    fi
fi

for service in player proxy relay; do
    cd cmd/$service
    goreleaser release
    cd ../..
done
