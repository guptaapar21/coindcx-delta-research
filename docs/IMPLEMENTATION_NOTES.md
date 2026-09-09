# Implementation notes

## Current GitHub storage model

GitHub Actions artifact storage is the constrained short-lived layer. Standard GitHub-hosted runners in public repositories are free and unlimited, while the GitHub Free plan lists 500 MB of included artifact storage. The collector therefore rotates raw artifacts into release assets before the artifact budget is exhausted. Storage housekeeping is a hard-fail operation: archival failures must not be swallowed, because a green workflow with no archival would silently accumulate artifacts.

## Why release assets are used for promoted archives

GitHub documents up to 1000 assets per release and a per-file limit of 2 GiB, with no total-size limit on a release. This makes a release asset a better long-term archive destination than keeping every raw batch as an Actions artifact. The archive manager splits a promotion into multiple assets if required.

## Local backup limitation

A GitHub-hosted workflow cannot directly place an archive into a user's local phone/PC filesystem. The included `tools/sync_archives.py` script bridges that gap by running on a machine the user controls.

## Scheduler reliability

The self-chain uses `repository_dispatch`, which GitHub documents as an event that creates a new workflow run even when the dispatch is made using `GITHUB_TOKEN`. The watchdog exists because any cloud workflow can fail or become stale.
