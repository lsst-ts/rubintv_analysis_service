#!/usr/bin/env bash
#
# Bring the deployment branches up to date with main.
#
# Each site's worker pod clones its deploy branch fresh at every container
# start, so merging main into a deploy branch *is* the deployment for that
# site. See doc/lsst.rubintv.analysis.service/deployment.rst.
#
# Usage:
#   scripts/update_deploy_branches.sh                  # dry run, all sites
#   scripts/update_deploy_branches.sh --push           # merge and push
#   scripts/update_deploy_branches.sh --push deploy-slac
#
# The default is a dry run: it reports what each branch would gain and exits
# without changing anything.

set -euo pipefail

# The live deployment branches. Note that origin also carries deploy-usdf and
# deploy-slac-comcam, which are stale predecessors and deliberately excluded —
# updating them would suggest they are deployed when they are not.
ALL_BRANCHES=(deploy-slac deploy-summit deploy-bts)

SOURCE_BRANCH="main"
REMOTE="origin"

push=false
branches=()

for arg in "$@"; do
    case "$arg" in
        --push) push=true ;;
        --help|-h) grep -m1 -B99 '^$' "$0" | tail -n +3 | cut -c3-; exit 0 ;;
        -*) echo "unknown option: $arg" >&2; exit 2 ;;
        *) branches+=("$arg") ;;
    esac
done

if [ ${#branches[@]} -eq 0 ]; then
    branches=("${ALL_BRANCHES[@]}")
fi

# Refuse to run with local modifications: the script switches branches, and
# uncommitted work would either block the checkout or be carried onto a deploy
# branch by accident.
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    echo "error: working tree has uncommitted changes; commit or stash first" >&2
    exit 1
fi

starting_branch=$(git rev-parse --abbrev-ref HEAD)
restore() { git checkout --quiet "$starting_branch" 2>/dev/null || true; }
trap restore EXIT

echo "Fetching $REMOTE..."
git fetch --quiet "$REMOTE"

$push || echo "(dry run — pass --push to actually merge and push)"
echo

failed=()

for branch in "${branches[@]}"; do
    if ! git rev-parse --verify --quiet "$REMOTE/$branch" >/dev/null; then
        echo "$branch: no such branch on $REMOTE — skipping"
        failed+=("$branch")
        echo
        continue
    fi

    incoming=$(git rev-list --count "$REMOTE/$branch..$REMOTE/$SOURCE_BRANCH")
    site_only=$(git rev-list --count "$REMOTE/$SOURCE_BRANCH..$REMOTE/$branch")

    echo "$branch: $incoming commit(s) to take from $SOURCE_BRANCH, $site_only site-only commit(s)"

    if [ "$incoming" -eq 0 ]; then
        echo "  already up to date"
        echo
        continue
    fi

    git log --oneline "$REMOTE/$branch..$REMOTE/$SOURCE_BRANCH" | sed 's/^/    /'

    if ! $push; then
        echo
        continue
    fi

    # Merge rather than rebase. Deploy branches are published and pulled by
    # running pods, so rewriting their history would leave a pod that cloned
    # mid-update on a commit that no longer exists.
    git checkout --quiet "$branch"
    git merge --quiet --ff-only "$REMOTE/$branch" 2>/dev/null || true

    if git merge --no-edit "$REMOTE/$SOURCE_BRANCH" >/dev/null 2>&1; then
        git push --quiet "$REMOTE" "$branch"
        echo "  merged and pushed"
    else
        # Leave the conflict in place on the branch for the operator to
        # resolve; continuing to the next site would hide it.
        echo "  CONFLICT — resolve on '$branch', then: git push $REMOTE $branch" >&2
        failed+=("$branch")
        trap - EXIT
        exit 1
    fi
    echo
done

if [ ${#failed[@]} -gt 0 ]; then
    echo "completed with problems: ${failed[*]}" >&2
    exit 1
fi
