#!/bin/sh

gremlin -e ParseGitHubArchive.groovy ./githubarchive vertices_full.txt edges_full.txt

time sort -S6G -u -t$'\t' -k1,1 -o vertices.txt vertices_full.txt
time sort -S6G -u -o edges.txt edges_full.txt

gremlin -e ImportGitHubArchive.groovy vertices.txt edges.txt
