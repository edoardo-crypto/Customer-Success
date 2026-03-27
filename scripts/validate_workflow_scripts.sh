#!/usr/bin/env bash
# validate_workflow_scripts.sh — ensures every Python script referenced
# in a GitHub Actions workflow is actually committed to the repo.
#
# Usage:
#   bash scripts/validate_workflow_scripts.sh          # local (git-tracked check)
#   bash scripts/validate_workflow_scripts.sh --ci     # CI (file-on-disk check)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
WORKFLOW_DIR="$REPO_ROOT/.github/workflows"
MODE="${1:-local}"

if [ ! -d "$WORKFLOW_DIR" ]; then
  echo "No .github/workflows/ directory — nothing to validate."
  exit 0
fi

# In local mode: only scan tracked workflow files (what will actually be pushed).
# In CI mode: scan all workflow files on disk (they're checked out from the commit).
if [ "$MODE" = "--ci" ]; then
  workflow_files=$(find "$WORKFLOW_DIR" -maxdepth 1 \( -name '*.yml' -o -name '*.yaml' \) | sort)
else
  workflow_files=$(git -C "$REPO_ROOT" ls-files '.github/workflows/*.yml' '.github/workflows/*.yaml' \
    | while IFS= read -r f; do echo "$REPO_ROOT/$f"; done)
fi

if [ -z "$workflow_files" ]; then
  echo "No workflow files found — nothing to validate."
  exit 0
fi

missing=()

while IFS= read -r wf; do
  [ -f "$wf" ] || continue

  # Extract "python3 <path>.py" but skip "python3 -c" (inline code)
  scripts=$(grep -oE 'python3[[:space:]]+[A-Za-z0-9_./-]+\.py' "$wf" 2>/dev/null \
    | awk '{print $2}' || true)

  for script in $scripts; do
    script="${script#./}"  # strip leading ./

    if [ "$MODE" = "--ci" ]; then
      [ -f "$REPO_ROOT/$script" ] || missing+=("$script  (in $(basename "$wf"))")
    else
      git -C "$REPO_ROOT" ls-files --error-unmatch "$script" >/dev/null 2>&1 \
        || missing+=("$script  (in $(basename "$wf"))")
    fi
  done
done <<< "$workflow_files"

if [ ${#missing[@]} -gt 0 ]; then
  echo ""
  echo "ERROR: Workflow files reference Python scripts not committed to git:"
  echo ""
  for m in "${missing[@]}"; do
    echo "  - $m"
  done
  echo ""
  echo "Fix: git add <script> && git commit"
  echo ""
  exit 1
fi

echo "All workflow script references verified."
