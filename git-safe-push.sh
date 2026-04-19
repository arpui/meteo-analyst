#!/bin/bash

set -euo pipefail

echo "=== Git safe push ==="
echo ""

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: this directory is not inside a git repository."
    exit 1
fi

BRANCH="$(git branch --show-current 2>/dev/null || true)"
if [ -z "$BRANCH" ]; then
    echo "Error: could not detect current branch."
    exit 1
fi

if ! git remote get-url origin >/dev/null 2>&1; then
    echo "Error: remote 'origin' does not exist."
    git remote -v || true
    exit 1
fi

GIT_DIR="$(git rev-parse --git-dir)"

if [ -d "$GIT_DIR/rebase-merge" ] || [ -d "$GIT_DIR/rebase-apply" ]; then
    echo "Error: a rebase is already in progress."
    exit 1
fi

if [ -f "$GIT_DIR/MERGE_HEAD" ]; then
    echo "Error: a merge is already in progress."
    exit 1
fi

if [ -n "$(git diff --name-only --diff-filter=U)" ]; then
    echo "Error: there are unresolved conflicts:"
    git diff --name-only --diff-filter=U
    exit 1
fi

echo "Repository status:"
git status
echo ""

echo "Ignored files (for reference):"
git status --ignored -s
echo ""

UNTRACKED_FILES="$(git ls-files --others --exclude-standard)"

if [ -n "$UNTRACKED_FILES" ]; then
    echo "Untracked files detected:"
    echo "$UNTRACKED_FILES"
    echo ""

    read -r -p "Do you want to add any of these to .gitignore? (y/n): " ignore_confirm
    if [ "$ignore_confirm" = "y" ]; then
        echo "Enter file or pattern to ignore (example: *.log or build/):"
        read -r ignore_pattern

        if [ -n "$ignore_pattern" ]; then
            touch .gitignore
            grep -qxF "$ignore_pattern" .gitignore || echo "$ignore_pattern" >> .gitignore
            echo "Added '$ignore_pattern' to .gitignore"
            echo ""
            echo "Updated ignored files:"
            git status --ignored -s
            echo ""
        else
            echo "No pattern entered."
            echo ""
        fi
    fi
fi

echo "Current branch: $BRANCH"
echo ""
echo "Remotes:"
git remote -v
echo ""

if [ "$BRANCH" = "main" ] || [ "$BRANCH" = "master" ]; then
    echo "Warning: you are on '$BRANCH'."
    read -r -p "Do you really want to continue? (y/n): " protect_confirm
    [ "$protect_confirm" = "y" ] || { echo "Operation cancelled."; exit 0; }
    echo ""
fi

read -r -p "Continue with add/commit/pull --rebase/push? (y/n): " confirm
[ "$confirm" = "y" ] || { echo "Operation cancelled."; exit 0; }

echo ""
echo "Adding changes..."
git add -A

if git diff --cached --quiet; then
    echo "No staged changes to commit."
else
    read -r -p "Enter commit message: " commit_msg
    if [ -z "$commit_msg" ]; then
        echo "Error: commit message cannot be empty."
        exit 1
    fi
    echo "Creating commit..."
    git commit -m "$commit_msg"
fi

echo ""
echo "Pulling latest changes with rebase from origin/$BRANCH..."
if ! git pull --rebase origin "$BRANCH"; then
    echo "Error: pull --rebase failed."
    echo "Use:"
    echo "  git rebase --continue"
    echo "or:"
    echo "  git rebase --abort"
    exit 1
fi

echo ""
echo "Status after pull --rebase:"
git status
echo ""

if [ -n "$(git diff --name-only --diff-filter=U)" ]; then
    echo "Error: conflicts detected after pull --rebase:"
    git diff --name-only --diff-filter=U
    exit 1
fi

read -r -p "Push now to origin/$BRANCH? (y/n): " push_confirm
[ "$push_confirm" = "y" ] || { echo "Push cancelled."; exit 0; }

echo ""
echo "Pushing to origin/$BRANCH..."
git push origin "$BRANCH"

echo ""
echo "Done successfully."
