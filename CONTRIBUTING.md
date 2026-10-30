# Contributing to qilat

Thanks for your interest in qilat. This document explains how to propose changes.

## Before you start

- For anything bigger than a small fix, open an issue first so the approach can be discussed.
- Check [API stability](README.md#api-stability) and the [changelog](CHANGELOG.md) to see where the change fits.
- Security issues go through [SECURITY.md](SECURITY.md), not public issues.

## Development setup

Requirements:

- Go 1.26 or newer
- `make`

Common commands:

| Command | What it does |
|---|---|
| `make fmt` | Format code with `gofmt` |
| `make lint` | Run `gofmt` check, `go vet`, and golangci-lint |
| `make test` | Run unit tests |
| `make race` | Run tests with the race detector |
| `make cover` | Run tests with coverage and print a summary |
| `make bench` | Run comparison benchmarks in `benchmarks/` |
| `make fuzz` | Run every fuzz target for `FUZZTIME` (default `30s`) |
| `make vuln` | Run `govulncheck` |

golangci-lint and govulncheck are run with `go run` at pinned versions, so no global install is needed.

## Pull requests

- Keep each pull request focused on one change.
- Add or update tests for every behavior change. Parsers and protocol code also need fuzz targets.
- Performance claims must include benchmark results (`go test -bench . -benchmem`, ideally compared with
  [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)).
- Do not use standard library APIs newer than the minimum Go version in `go.mod`; CI tests on it.
- Make sure `make lint race` passes locally.
- Update `README.md` when you add or change user-facing behavior. Every Go snippet in the README is also a
  program in `examples/readme`, and a test there fails when the two differ.
- Add an entry under `[Unreleased]` in `CHANGELOG.md` for user-facing changes, with migration notes for
  breaking ones.
- Pull requests are reviewed automatically by [CodeRabbit](https://coderabbit.ai). Comment
  `@coderabbitai review` to request a new review.

## Commit messages

Use a short imperative summary, optionally prefixed with the affected area:

```
h2: enforce SETTINGS_MAX_HEADER_LIST_SIZE
```

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).
