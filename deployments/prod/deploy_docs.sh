#!/bin/bash

# Retrieve the latest deployed version of the dbt docs site
# The version is the commit hash at the time of deployment
deployedCommit=$(gcloud app versions list --format="value(version.id)" --sort-by="~version.createTime" | head -n 1)

echo "Latest deployed version: $deployedCommit the latest commit is $COMMIT_SHA"

# Check whether the deployed commit hash matches with the current latest commit hash
if [ "$deployedCommit" != "$COMMIT_SHA" ]; then
    echo "Deploying dbt website ..."
    gcloud alpha builds triggers run analytics-docs --branch master
else
    echo "Docs site already up-to-date"
fi
