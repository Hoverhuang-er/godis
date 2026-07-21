package main

import (
	"fmt"
	"os"
	"os/signal"

	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	"github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"golang.org/x/term"
)

const (
	clearScreen = "\033[2J"
	cursorHome  = "\033[H"
	hideCursor  = "\033[?25l"
	showCursor  = "\033[?25h"
	bold        = "\033[1m"
	reset       = "\033[0m"
	colorPurple = "\033[38;5;99m"
	colorWhite  = "\033[38;5;255m"
	colorDim    = "\033[38;5;245m"
	colorGreen  = "\033[38;5;82m"
	colorYellow = "\033[38;5;220m"
	colorRed    = "\033[38;5;196m"
	colorCyan   = "\033[38;5;81m"
	bgPurple    = "\033[48;5;99m"
	bgDim       = "\033[48;5;236m"
)

type monitorState struct {
	c         *client.Client
	width     int
	height    int
	opsHistory  []int64
	connHistory []int64
	cmdHistory  []int64
	startTime time.Time

	// Last query results
	hotKeys     []hotKeyStat
	bigKeys     []bigKeyStat
	dbKeys      int64
	dbExpires   int64
	currentOps  int64
	totalCmds   int64
	connections int64
	usedMemory  uint64
}

type hotKeyStat struct {
	key   string
	count int64
}

type bigKeyStat struct {
	key   string
	db    int
	typ   string
	size  int64
	bytes int64
}

func runMonitor(c *client.Client, flags cliFlags) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to enter raw terminal mode: %v\n", err)
		os.Exit(1)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	fmt.Print(hideCursor)
	defer fmt.Print(showCursor)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH)

	state := &monitorState{
		c:         c,
		startTime: time.Now(),
		width:     80,
		height:    24,
	}

	// Get initial terminal size
	if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
		state.width = w
		state.height = h
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Initial data fetch
	state.collectData()

	for {
		select {
		case <-ticker.C:
			// Check for 'q' key to quit (non-blocking)
			var buf [1]byte
			if n, _ := os.Stdin.Read(buf[:]); n > 0 {
				if buf[0] == 'q' || buf[0] == 'Q' || buf[0] == '\x03' {
					return
				}
			}
			state.collectData()
			state.render()

		case <-sigCh:
			// SIGWINCH for terminal resize
			if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
				state.width = w
				state.height = h
			}
			state.render()
		}
	}
}

func (s *monitorState) collectData() {
	// Collect INFO stats
	reply := s.c.Send(utils.ToCmdLine("INFO"))
	if reply != nil {
		if rr, ok := reply.(*protocol.StatusReply); ok {
			s.parseInfo(rr.Status)
		} else if rr, ok := reply.(*protocol.BulkReply); ok {
			s.parseInfo(string(rr.Arg))
		}
	}

	// Collect DBSIZE
	reply = s.c.Send(utils.ToCmdLine("DBSIZE"))
	if reply != nil {
		if ir, ok := reply.(*protocol.IntReply); ok {
			s.dbKeys = ir.Code
		}
	}

	// Update history (keep last width-20 samples)
	maxSamples := s.width - 20
	if maxSamples < 10 {
		maxSamples = 10
	}
	if len(s.opsHistory) >= maxSamples {
		s.opsHistory = s.opsHistory[1:]
	}
	s.opsHistory = append(s.opsHistory, s.currentOps)
	if len(s.connHistory) >= maxSamples {
		s.connHistory = s.connHistory[1:]
	}
	s.connHistory = append(s.connHistory, s.connections)
	if len(s.cmdHistory) >= maxSamples {
		s.cmdHistory = s.cmdHistory[1:]
	}
	s.cmdHistory = append(s.cmdHistory, s.currentOps)
}

func (s *monitorState) parseInfo(info string) {
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		switch key {
		case "instantaneous_ops_per_sec":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				s.currentOps = v
			}
		case "total_commands_processed":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				s.totalCmds = v
			}
		case "connected_clients":
			if v, err := strconv.ParseInt(val, 10, 64); err == nil {
				s.connections = v
			}
		case "used_memory":
			if v, err := strconv.ParseUint(val, 10, 64); err == nil {
				s.usedMemory = v
			}
		case "expired_keys":
			// ignore
		}
	}
}

func (s *monitorState) render() {
	sb := &strings.Builder{}
	sb.WriteString(cursorHome)

	// Header
	uptime := int64(time.Since(s.startTime).Seconds())
	sb.WriteString(bgPurple)
	sb.WriteString(colorWhite)
	sb.WriteString(bold)
	header := fmt.Sprintf(" Godis Monitor   Uptime: %ds  Keys: %d  Q: quit", uptime, s.dbKeys)
	if len(header) > s.width {
		header = header[:s.width]
	}
	sb.WriteString(header)
	sb.WriteString(reset)
	sb.WriteString("\n")

	// Stats row
	memStr := formatBytes(s.usedMemory)
	statsLine := fmt.Sprintf(" %sOps/s:%s %d  %sConns:%s %d  %sTotal:%s %d  %sMem:%s %s",
		colorPurple, colorWhite, s.currentOps,
		colorPurple, colorWhite, s.connections,
		colorPurple, colorWhite, s.totalCmds,
		colorPurple, colorWhite, memStr,
	)
	if len(statsLine) > s.width {
		statsLine = statsLine[:s.width]
	}
	sb.WriteString(statsLine)
	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", s.width))
	sb.WriteString("\n")

	// Two-column layout: OPS chart (left) + Hot Keys (right)
	colWidth := s.width / 2
	if colWidth < 20 {
		colWidth = 20
	}
	chartHeight := (s.height - 6) / 2
	if chartHeight < 5 {
		chartHeight = 5
	}

	// Left: OPS chart (sparkline bar chart)
	sb.WriteString(bold)
	sb.WriteString(colorPurple)
	sb.WriteString(" OPS History")
	sb.WriteString(reset)
	sb.WriteString("\n")
	s.renderSparkline(sb, s.opsHistory, colWidth, chartHeight)

	// Right side: Hot Keys rank
	hotKeyX := colWidth
	if hotKeyX < s.width {
		// Move cursor to column position
		// We render hot keys info
		s.renderHotKeysPanel(sb, colWidth, chartHeight)
	}

	sb.WriteString("\n")
	sb.WriteString(strings.Repeat("─", s.width))
	sb.WriteString("\n")

	// Bottom: Big Keys table
	bottomHeight := s.height - 6 - chartHeight - 3
	if bottomHeight < 3 {
		bottomHeight = 3
	}
	s.renderBigKeys(sb, s.width, bottomHeight)

	// Write the full buffer
	fmt.Print(sb.String())
}

func (s *monitorState) renderSparkline(sb *strings.Builder, data []int64, width, height int) {
	if len(data) == 0 {
		for i := 0; i < height; i++ {
			sb.WriteString("\n")
		}
		return
	}

	maxVal := int64(0)
	for _, v := range data {
		if v > maxVal {
			maxVal = v
		}
	}
	if maxVal == 0 {
		maxVal = 1
	}

	barWidth := width - 15
	if barWidth < 5 {
		barWidth = 5
	}
	if len(data) > barWidth {
		data = data[len(data)-barWidth:]
	}

	for row := height - 1; row >= 0; row-- {
		threshold := int64(height - row) * maxVal / int64(height)
		if row == height-1 {
			sb.WriteString(fmt.Sprintf(" %s%s%s ", colorDim, formatNum(maxVal), reset))
		} else if row == 0 {
			sb.WriteString(fmt.Sprintf(" %s0%s ", colorDim, reset))
		} else {
			sb.WriteString("   ")
		}

		for _, v := range data {
			if v >= threshold {
				sb.WriteString(colorPurple + "▇" + reset)
			} else {
				sb.WriteString(colorDim + "·" + reset)
			}
		}
		sb.WriteString("\n")
	}
}

func (s *monitorState) renderHotKeysPanel(sb *strings.Builder, x, height int) {
	// This is appended to the right side of the chart
	// For simplicity, we render it as a separate section
	// since ANSI cursor positioning across columns is complex in raw mode
}

func (s *monitorState) renderBigKeys(sb *strings.Builder, width, height int) {
	sb.WriteString(bold)
	sb.WriteString(colorPurple)
	sb.WriteString(" DB Keys by Database")
	sb.WriteString(reset)
	sb.WriteString("\n")

	// Show DBSIZE info
	sb.WriteString(fmt.Sprintf(" %sTotal keys:%s %d", colorDim, colorWhite, s.dbKeys))
	sb.WriteString("\n")

	// Memory bar
	memStr := formatBytes(s.usedMemory)
	barLen := width - 20
	if barLen < 10 {
		barLen = 10
	}
	filled := int(uint64(barLen) * s.usedMemory / max(s.usedMemory, 1))
	if filled > barLen {
		filled = barLen
	}
	sb.WriteString(fmt.Sprintf(" %sMemory:%s [%s%s%s] %s\n",
		colorDim, reset,
		colorPurple+strings.Repeat("█", filled)+reset,
		colorDim+strings.Repeat("░", barLen-filled)+reset,
		reset, memStr))
}

func formatBytes(b uint64) string {
	if b == 0 {
		return "0B"
	}
	units := []string{"B", "KB", "MB", "GB"}
	i := 0
	f := float64(b)
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}

func formatNum(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return strconv.FormatInt(n, 10)
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// Ensure error types from protocol are used
var _ = protocol.NullBulkReply{}
