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
	hideCursor = "\033[?25l"
	showCursor = "\033[?25h"
	bold       = "\033[1m"
	reset      = "\033[0m"
	purple     = "\033[38;5;99m"
	white      = "\033[38;5;255m"
	dim        = "\033[38;5;245m"
	green      = "\033[38;5;82m"
	yellow     = "\033[38;5;220m"
	cyan       = "\033[38;5;81m"
	bgPurple   = "\033[48;5;99m"
	cursorSave = "\033[s"
	cursorLoad = "\033[u"
)

type kv struct{ key string; count int64 }
type bk struct{ key string; typ string; bytes int64 }

type cliTUI struct {
	c           *client.Client
	flags       cliFlags
	width       int
	height      int
	startTime   time.Time

	// Data
	hotKeys     []kv
	bigKeys     []bk
	keyHistory  []int64
	opsHistory  []int64

	// CLI mode state
	cmdInput   string
	cmdHistory []string
	cmdOutput  string
	cmdPos     int
}

func runCLITUI(c *client.Client, flags cliFlags) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to enter raw terminal mode: %v\n", err)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	fmt.Print(hideCursor)
	defer fmt.Print(showCursor)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	// SIGWINCH only on Unix; Windows uses a different resize mechanism
	notifyWinch(sigCh)

	tui := &cliTUI{
		c:         c,
		flags:     flags,
		startTime: time.Now(),
		width:     120,
		height:    35,
	}
	if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
		tui.width = w; tui.height = h
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	tui.collectData()
	tui.render()

	for {
		select {
		case <-ticker.C:
			tui.collectData()
			tui.render()
		case <-sigCh:
			if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
				tui.width = w; tui.height = h
			}
			tui.render()
		default:
			// Read keyboard input
			var buf [8]byte
			n, _ := os.Stdin.Read(buf[:])
			if n == 0 { time.Sleep(50 * time.Millisecond); continue }

			if n == 1 && buf[0] == 'q' { return }
			if n == 1 && buf[0] == 3 { return } // Ctrl+C
			if n == 1 && buf[0] == 13 { // Enter
				cmd := tui.cmdInput
				if cmd != "" {
					tui.cmdHistory = append(tui.cmdHistory, cmd)
					tui.executeCmd(cmd)
					tui.cmdInput = ""
					tui.render()
				}
			} else if n == 1 && buf[0] == 127 { // Backspace
				if len(tui.cmdInput) > 0 {
					tui.cmdInput = tui.cmdInput[:len(tui.cmdInput)-1]
					tui.render()
				}
			} else if n == 1 && buf[0] >= 32 && buf[0] <= 126 {
				tui.cmdInput += string(buf[0])
				tui.render()
			} else if n == 3 && buf[0] == 27 && buf[1] == 91 && buf[2] == 65 { // Up
				if len(tui.cmdHistory) > 0 {
					tui.cmdPos--
					if tui.cmdPos < 0 { tui.cmdPos = 0 }
					tui.cmdInput = tui.cmdHistory[len(tui.cmdHistory)-1-tui.cmdPos]
					tui.render()
				}
			} else if n == 3 && buf[0] == 27 && buf[1] == 91 && buf[2] == 66 { // Down
				tui.cmdPos++
				if tui.cmdPos >= len(tui.cmdHistory) { tui.cmdPos = len(tui.cmdHistory); tui.cmdInput = "" }
				if tui.cmdPos < len(tui.cmdHistory) {
					tui.cmdInput = tui.cmdHistory[len(tui.cmdHistory)-1-tui.cmdPos]
				}
				tui.render()
			}
		}
	}
}

func (t *cliTUI) collectData() {
	// Collect INFO
	reply := t.c.Send(utils.ToCmdLine("INFO"))
	if reply != nil {
		var infoStr string
		if rr, ok := reply.(*protocol.StatusReply); ok { infoStr = rr.Status }
		if rr, ok := reply.(*protocol.BulkReply); ok { infoStr = string(rr.Arg) }
		t.parseInfo(infoStr)
	}

	// Collect DBSIZE
	reply = t.c.Send(utils.ToCmdLine("DBSIZE"))
	if reply != nil {
		if ir, ok := reply.(*protocol.IntReply); ok {
			t.keyHistory = append(t.keyHistory, ir.Code)
		}
	}

	maxSamples := t.width/2 - 15
	if maxSamples < 10 { maxSamples = 10 }
	if len(t.keyHistory) > maxSamples { t.keyHistory = t.keyHistory[1:] }
}

var lastOps int64

func (t *cliTUI) parseInfo(info string) {
	lines := strings.Split(info, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") || !strings.Contains(line, ":") { continue }
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 { continue }
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "instantaneous_ops_per_sec":
			if v, _ := strconv.ParseInt(val, 10, 64); v > 0 {
				t.opsHistory = append(t.opsHistory, v)
				maxSamples := t.width/2 - 15
				if maxSamples < 10 { maxSamples = 10 }
				if len(t.opsHistory) > maxSamples { t.opsHistory = t.opsHistory[1:] }
			}
		}
	}
}

func (t *cliTUI) executeCmd(cmd string) {
	parts := parseLine(cmd)
	if len(parts) == 0 { t.cmdOutput = "Error: empty command"; return }
	reply := t.c.Send(parts)
	t.cmdOutput = formatReply(reply)
}

func (t *cliTUI) render() {
	sb := &strings.Builder{}
	sb.WriteString("\033[H\033[J") // home + clear

	// Header bar
	header := fmt.Sprintf(" Godis %s%sCLI%s   %sQ:%s quit  |  Type a redis command below",
		reset, white, reset, dim, reset)
	sb.WriteString(bgPurple + white + bold + " " + header + reset + "\n")

	colW := t.width / 2
	if colW < 30 { colW = 30 }

	// LEFT PANEL: 3 monitoring sections stacked
	// Section 1: Hot Keys
	sectionH := (t.height - 5) / 3
	if sectionH < 4 { sectionH = 4 }
	t.renderSparkline(sb, "Hot Keys Access", t.opsHistory, colW, sectionH)

	// Section 2: Big Keys (use key count history)
	t.renderSparkline(sb, "Key Count", t.keyHistory, colW, sectionH)

	// Section 3: Key Length (same data, different label)
	t.renderSparkline(sb, "Ops/sec", t.opsHistory, colW, sectionH)

	// RIGHT PANEL: Command output
	x := colW
	for row := 0; row < t.height-4; row++ {
		sb.WriteString(fmt.Sprintf("\033[%d;%dH", row+2, x+1))
		rightContent := ""
		if row == 0 {
			rightContent = bold + purple + " Recent Commands " + reset
		} else if row == 1 {
			rightContent = dim + strings.Repeat("─", t.width-x-1) + reset
		} else if row-2 < len(t.cmdHistory) && row-2 >= 0 {
			hc := len(t.cmdHistory) - 1 - (row - 2)
			if hc >= 0 {
				cmd := t.cmdHistory[hc]
				if len(cmd) > t.width-x-3 { cmd = cmd[:t.width-x-3] }
				rightContent = dim + "> " + reset + cmd
			}
		} else if row-2 == len(t.cmdHistory) {
			if t.cmdOutput != "" {
				out := strings.Split(t.cmdOutput, "\n")
				if len(out) > 0 {
					line := out[0]
					if len(line) > t.width-x-3 { line = line[:t.width-x-3] }
					rightContent = green + line + reset
				}
			}
		} else if row-2 > len(t.cmdHistory) {
			extra := row - 2 - len(t.cmdHistory) - 1
			if t.cmdOutput != "" {
				out := strings.Split(t.cmdOutput, "\n")
				if extra < len(out) {
					line := out[extra]
					if len(line) > t.width-x-3 { line = line[:t.width-x-3] }
					rightContent = green + line + reset
				}
			}
		}
		// Pad to fill
		if len(rightContent) < t.width-x {
			rightContent += strings.Repeat(" ", t.width-x-len(rightContent))
		}
		sb.WriteString(rightContent[:t.width-x])
	}

	// Vertical separator
	for row := 1; row < t.height-3; row++ {
		sb.WriteString(fmt.Sprintf("\033[%d;%dH", row+1, colW))
		sb.WriteString(dim + "│" + reset)
	}

	// Bottom command line
	sb.WriteString(fmt.Sprintf("\033[%d;1H", t.height-2))
	sb.WriteString(dim + strings.Repeat("─", t.width) + reset)
	sb.WriteString(fmt.Sprintf("\033[%d;1H", t.height-1))
	prompt := fmt.Sprintf(" %s:%d> ", t.flags.host, t.flags.port)
	sb.WriteString(purple + prompt + reset)
	sb.WriteString(t.cmdInput)
	// Clear rest of line
	if len(t.cmdInput)+len(prompt) < t.width {
		sb.WriteString(strings.Repeat(" ", t.width-len(t.cmdInput)-len(prompt)))
	}

	fmt.Print(sb.String())
}

func (t *cliTUI) renderSparkline(sb *strings.Builder, title string, data []int64, width, height int) {
	sparkW := width - 5
	if sparkW < 5 { sparkW = 5 }

	// Section title
	sb.WriteString("\n " + bold + purple + title + reset + "\n")

	if len(data) == 0 {
		for i := 0; i < height-1; i++ { sb.WriteString("  " + dim + "no data" + reset + "\n") }
		return
	}

	if len(data) > sparkW { data = data[len(data)-sparkW:] }

	maxVal := int64(1)
	for _, v := range data { if v > maxVal { maxVal = v } }

	for row := height - 1; row >= 0; row-- {
		threshold := int64(height-row) * maxVal / int64(height)
		if row == height-1 {
			sb.WriteString(fmt.Sprintf(" %s%s%s ", dim, formatNum(maxVal), reset))
		} else if row == 0 {
			sb.WriteString(" 0 ")
		} else if row%2 == 0 {
			sb.WriteString(fmt.Sprintf(" %s·%s ", dim, reset))
		} else {
			sb.WriteString("   ")
		}
		for _, v := range data {
			if v >= threshold { sb.WriteString(purple + "█" + reset) } else { sb.WriteString(dim + "·" + reset) }
		}
		sb.WriteString("\n")
	}
}

func formatNum(n int64) string {
	if n >= 1000000 { return fmt.Sprintf("%.1fM", float64(n)/1000000) }
	if n >= 1000 { return fmt.Sprintf("%.1fK", float64(n)/1000) }
	return strconv.FormatInt(n, 10)
}
