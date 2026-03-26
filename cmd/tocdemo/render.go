package main

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	symFile      = "○"
	symChunk     = "●"
	symEmbedding = "◆"
	symEmpty     = "·"

	arrowIdle   = "───▶"
	arrowActive = "═══▶"

	frameWidth = 64
)

type stageSnap struct {
	name        string
	symbol      string
	color       string
	buffered    int64
	capacity    int
	inFlight    int64
	transferred int64
	serviceTime time.Duration
}

type frameData struct {
	scenarioName string
	scenarioDesc string
	stages       [3]stageSnap
	pipelineWIP  int64
	done         int64
	total        int
	memoryKB     int64
	idlePct      float64 // constraint idle proxy, 0-100
	elapsed      time.Duration
}

// renderFrame produces one visual pipeline frame as a string.
// Pure function — no side effects.
func renderFrame(f frameData) string {
	var b strings.Builder

	// Top border
	b.WriteString("╭" + strings.Repeat("─", frameWidth) + "╮\n")

	// Header
	header := fmt.Sprintf(" %s%-28s%s  WIP: %-3d  Done: %d/%d",
		colorBold, f.scenarioName, colorReset,
		f.pipelineWIP, f.done, f.total)
	writeLine(&b, header)

	desc := fmt.Sprintf(" %s%s%s", colorDim, f.scenarioDesc, colorReset)
	writeLine(&b, desc)

	// Separator
	b.WriteString("├" + strings.Repeat("─", frameWidth) + "┤\n")
	writeBlank(&b)

	// Stage names with arrows
	stageLine := " "
	for i, st := range f.stages {
		if i > 0 {
			arrow := arrowIdle
			if st.transferred > 0 {
				arrow = colorBold + arrowActive + colorReset
			}
			stageLine += " " + arrow + " "
		}
		stageLine += st.color + st.symbol + colorReset + " " + st.name
	}
	// Final arrow
	lastArrow := arrowIdle
	if f.stages[2].transferred > 0 {
		lastArrow = colorBold + arrowActive + colorReset
	}
	stageLine += " " + lastArrow + " ✓"
	writeLine(&b, stageLine)

	// Buffer line
	bufLine := " "
	for i, st := range f.stages {
		if i > 0 {
			bufLine += "       " // arrow spacing
		}
		bufLine += "buf:" + renderBuf(st)
	}
	writeLine(&b, bufLine)

	// In-flight line
	inLine := " "
	for i, st := range f.stages {
		if i > 0 {
			inLine += "       "
		}
		inLine += fmt.Sprintf("in:%-2d %s", st.inFlight, fmtDur(st.serviceTime))
	}
	writeLine(&b, inLine)

	writeBlank(&b)

	// Constraint idle indicator
	idleStr := fmt.Sprintf(" constraint idle: %.0f%% (approx)", f.idlePct)
	if f.idlePct > 10 {
		idleStr += "  " + colorRed + "← STARVING" + colorReset
	} else if f.idlePct >= 0 {
		idleStr += "  " + colorGreen + "✓ protected" + colorReset
	}
	writeLine(&b, idleStr)

	// Stats
	tputPerSec := float64(f.stages[2].transferred) / tickRate.Seconds()
	memMB := f.memoryKB / 1024
	statsLine := fmt.Sprintf(" throughput %s %3.0f/s   memory %s %dMB",
		tputBar(tputPerSec, 10), tputPerSec,
		memBar(memMB, 10), memMB)
	writeLine(&b, statsLine)

	// Legend
	legend := fmt.Sprintf(" %s%s%s File  %s%s%s Chunk  %s%s%s Embedding",
		colorGreen, symFile, colorReset,
		colorYellow, symChunk, colorReset,
		colorBlue, symEmbedding, colorReset)
	writeLine(&b, legend)

	// Bottom border
	b.WriteString("╰" + strings.Repeat("─", frameWidth) + "╯\n")

	return b.String()
}

func renderBuf(st stageSnap) string {
	if st.capacity <= 16 {
		filled := int(st.buffered)
		if filled < 0 {
			filled = 0
		}
		if filled > st.capacity {
			filled = st.capacity
		}
		empty := st.capacity - filled
		return st.color + strings.Repeat(st.symbol, filled) + colorReset +
			strings.Repeat(symEmpty, empty)
	}
	return proportionalBar(st.buffered, int64(st.capacity), 10, st.color)
}

func proportionalBar(current, max int64, width int, color string) string {
	pct := safePct(float64(current), float64(max))
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled
	return color + strings.Repeat("█", filled) + colorReset +
		strings.Repeat("░", empty) +
		fmt.Sprintf(" %d", current)
}

func tputBar(value float64, width int) string {
	pct := safePct(value, 60)
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled
	color := colorGreen
	if pct >= 0.5 {
		color = colorYellow
	}
	return color + strings.Repeat("█", filled) + strings.Repeat("░", empty) + colorReset
}

func memBar(memMB int64, width int) string {
	maxMB := int64(itemWeightKB * defaultItems / 1024)
	if maxMB <= 0 {
		maxMB = 1
	}
	pct := safePct(float64(memMB), float64(maxMB))
	filled := int(math.Round(pct * float64(width)))
	if filled > width {
		filled = width
	}
	empty := width - filled
	color := colorGreen
	switch {
	case pct >= 0.5:
		color = colorRed
	case pct >= 0.15:
		color = colorYellow
	}
	return color + strings.Repeat("█", filled) + strings.Repeat("░", empty) + colorReset
}

func safePct(num, denom float64) float64 {
	if denom <= 0 {
		return 0
	}
	p := num / denom
	if p < 0 {
		return 0
	}
	if p > 1 {
		return 1
	}
	return p
}

func writeLine(b *strings.Builder, content string) {
	b.WriteString("│" + padRight(content, frameWidth) + "│\n")
}

func writeBlank(b *strings.Builder) {
	b.WriteString("│" + strings.Repeat(" ", frameWidth) + "│\n")
}

func padRight(s string, width int) string {
	visible := stripANSI(s)
	pad := width - len([]rune(visible))
	if pad <= 0 {
		return s
	}
	return s + strings.Repeat(" ", pad)
}

func stripANSI(s string) string {
	var b strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\033' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func fmtDur(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}

// renderSummaryLine produces a one-line result for the main screen.
func renderSummaryLine(name string, throughput float64, peakWIP int64, peakMemKB int64, idlePct float64) string {
	wipColor := colorGreen
	switch {
	case peakWIP > 100:
		wipColor = colorRed
	case peakWIP > 30:
		wipColor = colorYellow
	}
	return fmt.Sprintf("  %s%-28s%s  t/s: %5.0f  peak WIP: %s%3d%s  mem: %dMB  idle: %.0f%%",
		colorBold, name, colorReset,
		throughput,
		wipColor, peakWIP, colorReset,
		peakMemKB/1024,
		idlePct)
}
