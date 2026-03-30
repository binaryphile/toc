# Assembly Join Semantics

What assembly joins mean for TOC pipelines, and the contract for implementing them.

## Synchronization as a throughput limiter

In linear pipelines (I-plants), if the constraint has capacity and material is
released, the constraint uses it. In A-plants, material must arrive in the right
combination at the right time. The constraint can have excess capacity yet be
underutilized because of synchronization failure. This doesn't happen in linear
pipelines.

Two types of throughput limiters:

- **Capacity constraint**: a stage can't process fast enough (classical CCR).
  The constraint buffer addresses this.
- **Synchronization constraint**: assembly can't start because inputs aren't
  available in the right combination, even with excess capacity. The feeding
  buffer addresses this.

Traditional DBR addresses only capacity. A-plant DBR must address both.
Everything below -- variability amplification, sibling subordination, feeding
buffers, the two-component design -- follows from this distinction.

**Software parallel: "The Tail at Scale."** A request fanning out to N services
and collecting all responses has latency = max(service latencies). One straggler
delays everything (Dean & Barroso). Same math as branch availability at assembly.
Google's solution: speculative execution (redundancy). TOC's solution: feeding
buffers (time protection). Different mechanisms, same root cause --
synchronization at convergence points amplifies tail behavior.

## Goldratt's A-plant

Assembly join is the **A-plant** in Goldratt's plant-type classification (The
Race). Goldratt identified the A-plant's characteristic problems: component
shortages at assembly, excess WIP in non-constraining branches, long lead times
from synchronization. We're implementing established TOC theory for software
pipelines, not inventing new concepts.

Feeding buffers come from both DBR production scheduling and Critical Chain
Project Management (CCPM). In CCPM, feeding chains merge into the critical chain
at convergence points. Feeding buffers protect those points. In production
A-plants, feeder branches are feeding chains, the assembly node is the
convergence point, and branch buffers are feeding buffers. The downstream rope is
analogous to the project buffer (protects the drum/delivery date).

## Four claims

1. An assembly join is a fixed-ratio consumptive synchronization point that
   requires time-phased, ratio-aware control upstream and single-stream control
   downstream.
2. DBR remains a one-drum, one-rope policy, but software implementation
   decomposes into coordinated release components sharing one schedule and one
   release budget.
3. Feeding buffers at assembly are time-based protection against branch
   variability and response delay -- managed separately from the drum buffer but
   subordinated to the same drum schedule.
4. V1 does not support persistent upstream CCRs. Repeated evidence that a feeder
   branch is the true CCR is a reconfiguration signal, not a normal operating
   mode.

## Insight 1: Assembly joins change the control regime

The constraint is still singular and system-level. But the *control regime*
changes at the assembly boundary: upstream requires ratio-aware synchronization,
downstream uses single-stream product units.

Three levels of constraint-related events at assembly:

- **Designed drum location**: the drum is at or downstream of assembly (v1
  assumption). The policy is designed around this.
- **Transient starvation**: a feeder branch temporarily can't keep up. Assembly
  starves. Feeding buffers absorb this. Normal operating mode -- the system
  recovers.
- **Persistent CCR migration**: a feeder branch is consistently the system
  bottleneck. This means the designed drum placement is wrong. V1 treats this as
  a reconfiguration trigger, not a mode the controller handles.

The Analyzer must distinguish these: low utilization at assembly could be
transient starvation (feeding buffer issue), persistent feeder bottleneck
(policy invalidation), or genuine overcapacity (assembly is not the constraint).
Each requires different intervention.

## Insight 2: Variability amplification (worst-branch gating)

The Goal's central insight -- dependent events + statistical fluctuations =
cumulative delay -- hits hardest at assembly joins. Assembly throughput =
`min(branch_b arrival rate / recipe_b)`, not its own service capacity.
Variability on ANY branch starves the assembly.

With N independent branches, the probability of a complete recipe arriving on
time ~ `product(P(branch_b on time))`. Even 99% per-branch reliability with 5
branches = 95% assembly availability. More branches, more protection needed.
Buffer sizing must account for the convolution of all branch variabilities.

For correlated branches (shared infra, common scheduler): the independence
multiplication is invalid. Use empirical simulation or covariance-aware
estimation.

### Feeding buffer semantics

Branch buffers at assembly inputs are feeding buffers -- they protect
synchronization, not throughput. Different sizing principle from constraint
buffers.

- **Primary unit: time.** "Protect X time units of assembly demand on branch b."
- **Quantity per branch**: `FB_b = recipe_b * drum_rate * FB_b_time`
- **Sizing**: depends on branch variability, response/replenishment time, and
  target assembly service level.
- **Per-branch target**: for independent branches with assembly availability
  target alpha, per-branch target ~ `alpha^(1/N)`. Tightens as branch count
  grows.
- **Buffer penetration**: red/yellow/green zones per branch, based on coverage
  time remaining vs feeding buffer target. Penetration drives priority and
  escalation.
- **Interaction with drum buffer**: feeding buffers and drum buffer are both
  subordinated to the same drum schedule. Not additive "extra safety" --
  double-buffering inflates WIP.

### Prediction for Phase 2

The grid search on linear pipelines found "rope is dominant, buffer nearly flat."
Assembly joins should flip this -- feeding buffers become critical because a
missing input on one branch starves the whole assembly. The DES sim should test
whether buffer importance increases at assembly joins.

### RL research question

Give an RL agent per-branch release rates and per-branch feeding buffer targets.
Can it discover sibling subordination -- learning to coordinate releases to
maintain kit balance rather than maximizing individual branch throughput? If the
agent independently converges on a TOC-aligned subordination policy from reward
optimization alone, that's evidence the A-plant prescription is emergent, not
just prescribed.

## Insight 3: Sibling subordination

In linear DBR, each stage subordinates to the drum. With assembly, a fast feeder
branch must also subordinate to the *slowest sibling*. Releasing wheels ahead of
chassis builds unmatchable inventory. This is a new subordination relationship
absent in linear pipelines.

The feeder coordinator enforces it: no branch releases beyond what the
kit-equivalent balance allows. Each branch has a different flow time, a different
rope length, and staggered release timing. The longest branch is the critical
path; shorter branches have slack. Temporal alignment (arrivals within a bounded
window) matters as much as quantity alignment (correct ratio).

## The unit boundary

Assembly is where units change. Wheels are not chassis. Neither is an assembled
product. Three distinct domains:

| Domain | Units | Controller |
|--------|-------|------------|
| Branch-native | wheels, chassis | Feeder coordinator (per-branch) |
| Kit-equivalent | complete assemblable sets | Feeder coordinator (cross-branch) |
| Product | assembled units | Downstream rope |

Do not aggregate WIP across domains without explicit normalization.
50 wheels + 10 chassis is not "60 WIP" -- it's 10 kits with 10 excess wheels.

Kit-equivalent state (three separate quantities):

1. **Available kits** = `floor(min(Q_b / r_b))` -- starts immediately feasible
2. **Product-equivalent WIP per branch** = `branchWIP_b / r_b` -- in-flight
   equivalents
3. **Branch skew** = deviation from target kit-equivalents -- ahead/behind

## One rope, two components

In Goldratt's A-plant model, DBR is still one drum, one rope, one set of
buffers. The rope doesn't stop at the assembly node -- it passes through it with
multiple strands. Each strand releases at `drum_rate * recipe_b`. The assembly
node is a ratio-aware waypoint, not a boundary between separate control systems.

The implementation decomposes into two components -- not because of units (you
can convert via recipe coefficients) but because the *control policy* differs:

- **Downstream** (assembly to drum): single-stream WIP control. Little's Law
  heuristic. The existing `RopeController` handles this.
- **Upstream** (branch heads to assembly): coupled multi-input synchronization.
  Ratio alignment + temporal alignment + sibling subordination. Can't solve with
  N independent Little's Law controllers because the ratio constraint couples
  all branches.

One rope conceptually (Goldratt), two cooperating components implementing one
release policy. Normative invariants that make this provably one policy:

- One drum schedule / release budget in product-equivalent units (shared).
- Upstream branch release times derived from the same drum schedule via
  branch-specific offsets (lead time + feeding buffer target + recipe
  coefficient).
- Neither component optimizes independently -- the feeder coordinator is
  subordinate to the drum schedule, not an autonomous optimizer.
- Any branch release decision is bounded by the system-level release plan.

Without these invariants, it's just two controllers with a nicer story.

## Assembly node contract

An assembly node is explicitly declared (not topology-inferred). Contract:

- Atomic AND-join: service starts only when all branches provide the full recipe.
- Recipe = fixed positive integer coefficient per incoming edge.
- One start consumes the recipe atomically and produces one product unit.
- No partial consumption, optional inputs, rework, or scrap.
- Starvation = assembly blocks waiting for the limiting branch.
- Extra inventory on non-limiting branches buffers at the assembly input.

`AddEdgeWithRatio(from, to, ratio)` means: `ratio` items from `from` consumed
per start at `to`. This is a recipe coefficient on a consumptive sync-input
edge. NOT a general unit-conversion or cardinality-transform concept.

**API scope**: `AddEdgeWithRatio` can currently be called on any edge in the
Pipeline, not just assembly-input edges. For v1, ratios on non-assembly edges
have no control semantics -- the feeder coordinator only consumes ratios on
incoming edges to the declared assembly node. Ratios elsewhere are ignored by
the controller.

## V1 topology constraints

- One declared assembly node in the controlled subgraph.
- Each feeder branch: linear chain from branch control stage to assembly input.
- Drum at or downstream of assembly node (upstream-drum excluded).
- No tees, diamonds, or shared stages within the controlled feeder subgraph.
- Fixed positive integer recipe on all assembly-input edges.
- Each branch has exactly one path to the assembly node.

## Out of scope

- Dynamic/runtime-varying recipes (need demand-signal interface, not topology
  ratios).
- Variable cardinality (1 file to N chunks) -- different concept, not a recipe.
- Multiple assembly nodes in series.
- Drum upstream of assembly node.
- Optional inputs, yield loss, probabilistic routing.
- Topology-inferred assembly detection.

## Planner acceptance rules

How the topology planner validates a declared assembly node (normative for
Phase 1):

- Assembly node must be explicitly declared (not inferred from in-degree).
- All incoming edges to the assembly node must have positive integer recipe
  coefficients.
- Each incoming edge's source must be reachable from exactly one controlled
  branch head via a unique linear path (no diamonds, no shared stages).
- No tees within any controlled branch.
- Drum must be the assembly node or reachable downstream via a linear path.
- Assembly node must have out-degree = 1 (single product stream, v1).
- Rejection with actionable error if any condition fails.

Metadata computed by planner:
- Per-branch: head stage, path to assembly, effective recipe coefficient, branch
  flow time estimate.
- Cross-branch: recipe vector, total recipe cost, branch count.
- Downstream: assembly-to-drum segment (for existing RopeController).

## DES event semantics

How the assembly node processes items in the discrete-event simulation (normative
for Phase 2):

- Assembly node has workers (servers) and per-branch input queues (FIFO).
- An assembly start is *enabled* when: a free server exists AND all branch input
  queues have at least `recipe_b` items.
- On start: atomically consume `recipe_b` items from each branch queue. Draw
  service time. Schedule completion.
- On completion: emit one product-unit item downstream. Free server. Check if
  another start is enabled.
- Starvation: if server is free but recipe incomplete, record starvation event
  attributed to the short branch(es).
- Blocking: if downstream is full (backpressure), standard blocked-after-service
  semantics.
- Stats: assembly starts, completions, per-branch consumption, starvation events
  (per-branch attribution), available kits, utilization.

Release timing in DES:
- Each branch has its own source with independent arrival process.
- Rope controls per-branch release rate (feeder coordinator).
- Release timing for branch b: `drum_rate * recipe_b` items per interval, offset
  by branch lead time.

## Required observability

These must be instrumentable in Phase 2 (DES) and Phase 3 (library):

- Assembly starts/interval, starvation events (per-branch attribution).
- Per-branch queue depth at assembly input, available kits, coverage time.
- Per-branch product-equivalent WIP, branch skew (time-phased).
- Feeding buffer penetration (red/yellow/green per branch).
- Assembly utilization (starts / capacity).
- Invalidation signal: repeated deep penetration across intervals suggesting CCR
  migration.

## Worked examples

### Supported: downstream drum

```
chassis (r=1) --+
                +--> assembly --> paint(drum) --> ship
wheels  (r=4) --+
```

Scenario walkthrough:

- **Steady state**: paint drum at 10/interval. Chassis releases 10/interval,
  wheels 40/interval. Available kits = min(chassisQ/1, wheelsQ/4) stays healthy.

- **Variability spike**: wheels branch service-time spike. Wheel arrivals drop to
  20/interval for 3 intervals. Available kits = min(10, 20/4) = min(10, 5) = 5.
  Assembly throughput halves. (Worst-branch gating.)

- **Feeding buffer absorbs**: wheel feeding buffer held 80 wheels (2 intervals of
  protection). Assembly continues at full rate for 2 intervals, then degrades.
  Buffer sized to branch variability, not average throughput.

- **Sibling subordination**: chassis releases reduce to the time-phased
  requirement implied by the drum schedule and wheel-buffer penetration --
  schedule-subordinated release, not reactive rate-matching. Releasing 10
  chassis/interval while only 5 kits are feasible builds 5 unmatchable
  chassis/interval.

- **Reconfiguration trigger**: if the spike persists, repeated deep wheel-buffer
  penetration signals that the wheels branch may be the persistent CCR. V1
  treats this as a policy invalidation, not a mode to handle dynamically.

### Supported: assembly IS the drum

```
chassis (r=1) --+
                +--> assembly(drum) --> ship
wheels  (r=4) --+
```

The simplest v1 case:

- Feeding buffers and constraint buffers are the same (branch queues protect both
  synchronization and the constraint).
- Downstream rope degenerates (no segment between drum and exit).
- Only the feeder coordinator matters.
- Starvation at assembly = direct throughput loss (no downstream buffer to
  absorb).

### Rejected topologies

- **Diamond**: shared ancestors across branches create attribution ambiguity and
  WIP double-counting.
- **Drum upstream of assembly**: feeder controller would need to also be the
  drum's rope -- fundamentally different control problem.
- **Tee within feeder branch**: fan-out within the controlled region creates
  multiple paths to assembly, breaking the unique-path invariant.
