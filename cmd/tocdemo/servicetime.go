package main

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"time"
)

// itemDuration returns a deterministic service time for a given
// (seed, itemID, stage) triple. Each triple produces an independent
// RNG — no order dependence, no shared state, refactor-safe.
//
// With cv=0, returns mean exactly (deterministic).
func itemDuration(seed int64, itemID int, stage string, mean time.Duration, cv float64) time.Duration {
	if cv == 0 {
		return mean
	}
	h := fnv.New64a()
	binary.Write(h, binary.LittleEndian, seed)
	binary.Write(h, binary.LittleEndian, int64(itemID))
	io.WriteString(h, stage)
	rng := rand.New(rand.NewSource(int64(h.Sum64())))
	return logNormalDuration(mean, cv, rng)
}

// logNormalDuration draws a log-normal sample with the given mean and
// coefficient of variation. Floors at 1ms to avoid sub-millisecond
// sleeps dominated by scheduler noise.
func logNormalDuration(mean time.Duration, cv float64, rng *rand.Rand) time.Duration {
	meanF := float64(mean)
	sigma2 := math.Log(1 + cv*cv)
	sigma := math.Sqrt(sigma2)
	mu := math.Log(meanF) - sigma2/2
	sample := math.Exp(mu + sigma*rng.NormFloat64())
	d := time.Duration(sample)
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}
