# DeltaFlow agent instructions

These instructions apply to every task in this repository.

## Required delivery workflow

For an implementation task, carry the change through integration instead of
stopping after editing files:

1. Inspect the working tree and preserve unrelated user changes.
2. Run tests and checks appropriate to the changed area. Run the full test suite
   when practical.
3. Review the final diff for scope, generated files, secrets, and accidental
   artifacts.
4. Commit the completed change on the task's feature branch with a concise,
   descriptive commit message.
5. Push the feature branch to `origin`.
6. Open a GitHub pull request targeting `main`. Include a short summary and the
   exact validation commands/results. A GitHub PR is the project's equivalent
   of a merge request (MR).
7. If required checks pass, the branch has no unresolved conflicts, and GitHub
   permits the operation, merge the PR into `main` automatically. Delete the
   remote feature branch after merging when safe.
8. Report the PR link, merge result, tests, and resulting `main` commit.

The repository owner has pre-authorized this normal commit/push/PR/merge workflow
for completed DeltaFlow tasks, so do not pause merely to request confirmation at
each of those steps.

## Merge safety

- Never merge with failing or pending required checks.
- Never bypass branch protection, required reviews, or security checks.
- Never force-push `main` or rewrite published history.
- Do not merge draft, incomplete, speculative, or partially tested work.
- If CI fails, diagnose and fix failures within task scope, push the fix, and
  wait for checks again.
- If a conflict or permission/authentication issue cannot be resolved safely,
  leave the PR open and report the exact blocker.
- Destructive migrations, data deletion, credential changes, releases, and
  deployments still require task-specific authorization when not explicitly in
  scope.

## Integration conventions

- Default branch: `main`.
- Remote: `origin` (`git@github.com:bluefateludi/deltaflow.git`).
- Prefer small, reviewable PRs with one responsibility.
- Use squash merge unless preserving multiple commits materially improves the
  history.
- Keep the Python import package and executable name `qsync` until a dedicated
  compatibility-aware rename task changes them; use **DeltaFlow** as the project
  display name.

