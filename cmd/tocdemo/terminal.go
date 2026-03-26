package main

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorGreen  = "\033[32m"
	colorBlue   = "\033[34m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorReset  = "\033[0m"
)

type terminal struct {
	isTTY   bool
	noColor bool
}

func newTerminal() terminal {
	fi, err := os.Stdout.Stat()
	isTTY := err == nil && fi.Mode()&os.ModeCharDevice != 0
	_, noColor := os.LookupEnv("NO_COLOR")
	t := terminal{isTTY: isTTY, noColor: noColor}
	if noColor || !isTTY {
		disableColors()
	}
	return t
}

func disableColors() {
	colorRed = ""
	colorYellow = ""
	colorGreen = ""
	colorBlue = ""
	colorBold = ""
	colorDim = ""
	colorReset = ""
}

func (t terminal) enterAltScreen() {
	if t.isTTY {
		fmt.Print("\033[?1049h")
	}
}

func (t terminal) exitAltScreen() {
	if t.isTTY {
		fmt.Print("\033[?1049l")
	}
}

func (t terminal) hideCursor() {
	if t.isTTY {
		fmt.Print("\033[?25l")
	}
}

func (t terminal) showCursor() {
	if t.isTTY {
		fmt.Print("\033[?25h")
	}
}

func (t terminal) home() {
	if t.isTTY {
		fmt.Print("\033[H")
	}
}

// setupCleanup registers signal handlers and returns a cleanup function
// that restores terminal state. Safe to call multiple times (sync.Once).
func setupCleanup(term terminal) func() {
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			term.exitAltScreen()
			term.showCursor()
		})
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cleanup()
		os.Exit(1)
	}()

	return cleanup
}
