#!/bin/bash
set -euo pipefail

echo "On branch: $(git rev-parse --abbrev-ref HEAD)"

git add -A
STAGED=$(git diff --cached --name-only || true)
if [ -z "$STAGED" ]; then
  echo "No changes to commit"
else
  git commit -m "feat(hysplit): add clustering job and make run/parse robust - impute persist HDFS timestamps"
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)

# Push, setting upstream if needed
git push --set-upstream origin "$BRANCH"
