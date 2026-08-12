# toc

Theory of Constraints Drum-Buffer-Rope pipeline engine for Go.

```go
import "github.com/binaryphile/toc"
```

## What it does

`toc` provides bounded pipeline stages with backpressure, constraint detection, and dynamic WIP control based on Goldratt's Theory of Constraints.

- **Stage** — bounded input queue + workers with concurrency control
- **Pipe** — compose stages into pipelines with error passthrough
- **NewBatcher / NewWeightedBatcher** — accumulate items between stages
- **NewTee** — broadcast to N branches (lockstep)
- **NewMerge** — fan-in from multiple sources
- **NewJoin** — recombine two branch results

## Packages

| Package | Purpose |
|---------|---------|
| `toc` | Stage runner and pipeline composition |
| `core` | Deterministic constraint analyzer (ClassifyStep, Analyzer) |
| `analyze` | Rebalancer for runtime WIP adjustment |
| `tocpb` | Protobuf wire types + converters |

## Quick start

```go
ctx := context.Background()

// Three-stage pipeline: parse → transform → store
parse := toc.Start(ctx, parseFn, toc.Options[string]{Capacity: 100, Workers: 4})
transform := toc.Pipe(ctx, parse.Out(), transformFn, toc.Options[Parsed]{Capacity: 50, Workers: 2})
store := toc.Pipe(ctx, transform.Out(), storeFn, toc.Options[Transformed]{Capacity: 50, Workers: 4})

// Submit work
for _, item := range items {
    parse.Submit(item)
}
parse.CloseInput()

// Drain results
for r := range store.Out() {
    if r.IsOk() {
        fmt.Println("stored:", r.Get())
    }
}
store.Wait()
```

## Examples

See `examples/` for runnable demos:
- `drum-demo` — why identifying the correct constraint matters
- `migration-sim` — constraint migration with rope teardown
- `pipeline_fanout.go` — DAG pipeline with Tee to DB + audit log
- `tee_join_wal.go` — write-ahead log pattern with Tee/Join

## License

MIT
