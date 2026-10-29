#!/usr/bin/env bash
# Runs every Fuzz* target in the main module for FUZZTIME each; go test accepts only one target per run.
set -euo pipefail

fuzztime="${FUZZTIME:-30s}"
found=0

for pkg in $(go list ./...); do
	for target in $(go test -list '^Fuzz' "$pkg" | grep '^Fuzz' || true); do
		found=1
		echo "==> $pkg $target ($fuzztime)"
		go test -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzztime" "$pkg"
	done
done

if [ "$found" -eq 0 ]; then
	echo "no fuzz targets found"
fi
