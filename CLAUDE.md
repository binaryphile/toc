# Tandem Protocol
@~/projects/tandem-protocol/README.md

# toc

Theory of Constraints DBR pipeline engine for Go.

## Dev

```bash
go build ./...
go test -race ./...
go vet ./...
buf generate              # regenerate tocpb/toc.pb.go from proto/toc/v1/toc.proto
```

## Architecture

- **toc** (root) — Stage runner, pipeline composition (Start, Pipe, NewBatcher, NewTee, NewMerge, NewJoin)
- **core** — Deterministic analyzer: ClassifyStep (pure), Analyzer.Step, Diagnosis
- **analyze** — Rebalancer consuming core.Diagnosis for runtime WIP adjustment
- **tocpb** — Protobuf types (StageObservation, Diagnosis) + hand-written converters

Key design: Pipeline topology is passive metadata (name + func() Stats), not stage owner. Analyzer, Rope, Rebalancer are independent — composed by consumer, not coupled. Drum is dynamic (in Analyzer), topology is static (in Pipeline).

All naming traces to Goldratt's TOC terminology. Do not rename away from TOC terms.

## Dependencies

Cross-repo deps on `github.com/binaryphile/fluentfp`:
- `rslt` — Result type for pipeline channels
- `memctl` — cgroup memory monitoring for memory rope

## Testing: Khorikov Principles

| Quadrant | Test Strategy |
|----------|---------------|
| Domain/Algorithms | Unit test heavily |
| Controllers | ONE integration test |
| Trivial | Don't test |
| Overcomplicated | Refactor first |

Concurrency-heavy code — use `-race` flag always.

## Branching

Trunk-based. Commit to main. Tag releases with semver.

### evtctl — project task management

```
evtctl task <description>            # publish a task event
evtctl task --to <project> <desc>    # task for another project
evtctl inbox <app> <message>         # send inbox message
evtctl done <id>[,<id>...] [evidence] # publish a task-done event
evtctl open                          # list open tasks
evtctl audit                         # full task reconciliation
evtctl claim <id> <name>             # claim a task
evtctl claims                        # list active claims
```

Stream name automatically derived from project directory: `tasks.toc`. To send tasks to other projects: `evtctl task --to <project> <description>`. To send inbox messages: `evtctl inbox <app> <message>`.
