#!/bin/sh

export P4PORT="perforce.eigenbase.org:1666"
export P4USER="guest"
export P4PASSWD=""
export P4CLIENT="guest"

export PATH="/build/git_repos:/build:$PATH" # p4 and our git-p4 should be in the path
