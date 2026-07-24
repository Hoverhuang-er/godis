package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	rclient "github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
	"golang.org/x/term"
)

const (
	migrateHideCursor = "\033[?25l"
	migrateShowCursor = "\033[?25h"
)

type migrateFlags struct {
	destHost string
	destPort int
	destPass string
	destDB   int
	srcPass  string
}

func parseMigrateFlags(args []string) migrateFlags {
	f := migrateFlags{
		destHost: "127.0.0.1",
		destPort: 6379,
		destDB:   0,
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--migrate":
		case "-dh":
			if i+1 < len(args) {
				i++
				f.destHost = args[i]
			}
		case "-dp":
			if i+1 < len(args) {
				i++
				if p, err := strconv.Atoi(args[i]); err == nil {
					f.destPort = p
				}
			}
		case "-da":
			if i+1 < len(args) {
				i++
				f.destPass = args[i]
			}
		case "-ddb":
			if i+1 < len(args) {
				i++
				if d, err := strconv.Atoi(args[i]); err == nil {
					f.destDB = d
				}
			}
		case "-sa":
			if i+1 < len(args) {
				i++
				f.srcPass = args[i]
			}
		}
	}
	return f
}

func runMigrate() {
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: godis --migrate <full|inc> -dh <host> -dp <port> [-da <password>] [-ddb <db>] [-sa <src-password>]\n")
		os.Exit(1)
	}

	subCmd := ""
	for _, arg := range args {
		if arg == "full" || arg == "inc" {
			subCmd = arg
			break
		}
	}
	if subCmd == "" {
		fmt.Fprintf(os.Stderr, "missing subcommand: full or inc\n")
		os.Exit(1)
	}

	flags := parseMigrateFlags(args)

	// Connect to local source (godis at 127.0.0.1:6379)
	srcAddr := net.JoinHostPort("127.0.0.1", "6379")
	src, err := rclient.MakeClient(srcAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to source godis at %s: %v\n", srcAddr, err)
		os.Exit(1)
	}
	src.Start()
	defer src.Close()

	if flags.srcPass != "" {
		reply := src.Send(utils.ToCmdLine("AUTH", flags.srcPass))
		if isErrorReply(reply) {
			fmt.Fprintf(os.Stderr, "source AUTH failed: %s\n", extractError(reply))
			os.Exit(1)
		}
	}

	// Connect to destination Redis
	destAddr := net.JoinHostPort(flags.destHost, strconv.Itoa(flags.destPort))
	dest, err := rclient.MakeClient(destAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to destination at %s: %v\n", destAddr, err)
		os.Exit(1)
	}
	dest.Start()
	defer dest.Close()

	if flags.destPass != "" {
		reply := dest.Send(utils.ToCmdLine("AUTH", flags.destPass))
		if isErrorReply(reply) {
			fmt.Fprintf(os.Stderr, "destination AUTH failed: %s\n", extractError(reply))
			os.Exit(1)
		}
	}

	// Select destination DB
	if flags.destDB > 0 {
		dest.Send(utils.ToCmdLine("SELECT", strconv.Itoa(flags.destDB)))
	}

	switch subCmd {
	case "full":
		runFullMigrateUI(src, dest, flags.destDB)
	case "inc":
		runIncMigrate(src, dest, flags.destDB)
	}
}

// --- Full Migration TUI ---

type fullMigrateUI struct {
	src        *rclient.Client
	dest       *rclient.Client
	destDB     int
	width      int
	height     int
	srcKeys    []string
	destKeys   []string
	srcValues  []string
	destValues []string
	total      int
	migrated   atomic.Int64
	completed  bool
	errMsg     string
	cursor     int
	scrollOff  int
}

func runFullMigrateUI(src, dest *rclient.Client, destDB int) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to enter raw terminal: %v\n", err)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	fmt.Print(migrateHideCursor)
	defer fmt.Print(migrateShowCursor)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH)

	ui := &fullMigrateUI{
		src:    src,
		dest:   dest,
		destDB: destDB,
		width:  140,
		height: 40,
	}
	if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
		ui.width = w
		ui.height = h
	}

	fmt.Fprintf(os.Stderr, "scanning source keys...\n")
	ui.srcKeys = scanAllKeys(src)
	ui.total = len(ui.srcKeys)
	ui.srcValues = make([]string, ui.total)
	ui.destValues = make([]string, ui.total)

	// Start migration in background
	migrateDone := make(chan struct{})
	go func() {
		ui.migrateAll()
		close(migrateDone)
	}()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	ui.collectDestKeys()
	ui.renderFull()

	for {
		select {
		case <-migrateDone:
			ui.completed = true
			ui.renderFull()
			// Wait for user to press 'q'
			for {
				var buf [8]byte
				n, _ := os.Stdin.Read(buf[:])
				if n > 0 && (buf[0] == 'q' || buf[0] == 3) {
					return
				}
			}
		case <-ticker.C:
			ui.collectDestKeys()
			ui.renderFull()
		case <-sigCh:
			if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
				ui.width = w
				ui.height = h
			}
			ui.renderFull()
		default:
			var buf [8]byte
			n, _ := os.Stdin.Read(buf[:])
			if n == 0 {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if n == 1 && buf[0] == 'q' {
				return
			}
			if n == 1 && buf[0] == 3 {
				return
			}
			// Scroll
			if n == 3 && buf[0] == 27 && buf[1] == 91 {
				switch buf[2] {
				case 65: // Up
					if ui.cursor > 0 {
						ui.cursor--
					}
					ui.renderFull()
				case 66: // Down
					if ui.cursor < ui.total-1 {
						ui.cursor++
					}
					ui.renderFull()
				}
			}
		}
	}
}

func (ui *fullMigrateUI) collectDestKeys() {
	ui.destKeys = scanAllKeys(ui.dest)
	destSet := make(map[string]bool, len(ui.destKeys))
	for _, k := range ui.destKeys {
		destSet[k] = true
	}

	// Pre-fetch dest values for keys that exist
	ui.destValues = make([]string, ui.total)
	for i, key := range ui.srcKeys {
		if destSet[key] {
			ui.destValues[i] = getKeyValue(ui.dest, key)
		}
	}
}

func (ui *fullMigrateUI) migrateAll() {
	for i, key := range ui.srcKeys {
		val := getKeyValue(ui.src, key)
		ui.srcValues[i] = val

		// Read key TTL
		var ttlMs int64
		ttlReply := ui.src.Send(utils.ToCmdLine("TTL", key))
		if ir, ok := ttlReply.(*protocol.IntReply); ok {
			ttlMs = ir.Code
		}

		migrateKey(ui.dest, ui.src, key, ttlMs)

		ui.migrated.Add(1)
	}
}

func scanAllKeys(c *rclient.Client) []string {
	var keys []string
	var cursor int64
	for {
		reply := c.Send(utils.ToCmdLine("SCAN", strconv.FormatInt(cursor, 10)))
		if reply == nil {
			break
		}
		mrr, ok := reply.(*protocol.MultiRawReply)
		if !ok || len(mrr.Replies) < 2 {
			break
		}
		// Parse cursor from first element
		if cursorReply, ok := mrr.Replies[0].(*protocol.BulkReply); ok && cursorReply.Arg != nil {
			if c, err := strconv.ParseInt(string(cursorReply.Arg), 10, 64); err == nil {
				cursor = c
			}
		}
		// Parse keys from second element
		if keyList, ok := mrr.Replies[1].(*protocol.MultiBulkReply); ok {
			for _, arg := range keyList.Args {
				if arg != nil {
					keys = append(keys, string(arg))
				}
			}
		}
		if cursor == 0 {
			break
		}
	}
	return keys
}

func getKeyValue(c *rclient.Client, key string) string {
	reply := c.Send(utils.ToCmdLine("DUMP", key))
	if reply == nil {
		return "(nil)"
	}
	br, ok := reply.(*protocol.BulkReply)
	if !ok || br.Arg == nil {
		// Try GET for string type fallback
		reply2 := c.Send(utils.ToCmdLine("GET", key))
		if br2, ok2 := reply2.(*protocol.BulkReply); ok2 && br2.Arg != nil {
			return fmt.Sprintf("\"%s\"", string(br2.Arg))
		}
		return "(nil)"
	}
	return fmt.Sprintf("(dump %d bytes)", len(br.Arg))
}

func migrateKey(dest, src *rclient.Client, key string, ttlMs int64) {
	// Get key type first
	typeReply := src.Send(utils.ToCmdLine("TYPE", key))
	keyType := ""
	if sr, ok := typeReply.(*protocol.StatusReply); ok {
		keyType = sr.Status
	}

	switch keyType {
	case "string":
		reply := src.Send(utils.ToCmdLine("GET", key))
		if br, ok := reply.(*protocol.BulkReply); ok && br.Arg != nil {
			dest.Send(utils.ToCmdLine("SET", key, string(br.Arg)))
		}
	case "hash":
		reply := src.Send(utils.ToCmdLine("HGETALL", key))
		if mbr, ok := reply.(*protocol.MultiBulkReply); ok && len(mbr.Args) > 0 {
			args := []string{"HMSET", key}
			for _, arg := range mbr.Args {
				if arg != nil {
					args = append(args, string(arg))
				}
			}
			dest.Send(convertToCmdLine(args))
		}
	case "list":
		reply := src.Send(utils.ToCmdLine("LRANGE", key, "0", "-1"))
		if mbr, ok := reply.(*protocol.MultiBulkReply); ok {
			for _, arg := range mbr.Args {
				if arg != nil {
					dest.Send(utils.ToCmdLine("RPUSH", key, string(arg)))
				}
			}
		}
	case "set":
		reply := src.Send(utils.ToCmdLine("SMEMBERS", key))
		if mbr, ok := reply.(*protocol.MultiBulkReply); ok {
			args := []string{"SADD", key}
			for _, arg := range mbr.Args {
				if arg != nil {
					args = append(args, string(arg))
				}
			}
			dest.Send(convertToCmdLine(args))
		}
	case "zset":
		reply := src.Send(utils.ToCmdLine("ZRANGE", key, "0", "-1", "WITHSCORES"))
		if mbr, ok := reply.(*protocol.MultiBulkReply); ok {
			args := []string{"ZADD", key}
			for _, arg := range mbr.Args {
				if arg != nil {
					args = append(args, string(arg))
				}
			}
			dest.Send(convertToCmdLine(args))
		}
	}

	// Set TTL if key has one
	if ttlMs > 0 {
		dest.Send(utils.ToCmdLine("EXPIRE", key, strconv.FormatInt(ttlMs, 10)))
	}
}

func (ui *fullMigrateUI) renderFull() {
	sb := &strings.Builder{}

	// Calculate layout
	leftW := ui.width/2 - 2
	if leftW < 20 {
		leftW = 20
	}
	rightW := ui.width - leftW - 3
	if rightW < 20 {
		rightW = 20
	}
	listH := ui.height - 6
	if listH < 3 {
		listH = 3
	}

	// Header
	migrated := ui.migrated.Load()
	pct := 0.0
	if ui.total > 0 {
		pct = float64(migrated) / float64(ui.total) * 100
	}

	header := fmt.Sprintf(" Data Migration - Full   Keys: %d/%d (%.1f%%)   Press 'q' to quit", migrated, ui.total, pct)
	sb.WriteString("\033[H\033[J") // home + clear
	sb.WriteString("\033[48;5;99m\033[38;5;255m\033[1m " + header + "\033[0m\n")

	// Progress bar
	if ui.total > 0 {
		barW := ui.width - 4
		if barW > 80 {
			barW = 80
		}
		filled := int(float64(barW) * pct / 100.0)
		sb.WriteString(" ")
		sb.WriteString("\033[38;5;245m[\033[0m")
		sb.WriteString("\033[48;5;82m" + strings.Repeat(" ", filled) + "\033[0m")
		sb.WriteString(strings.Repeat(" ", barW-filled))
		sb.WriteString("\033[38;5;245m]\033[0m")
		sb.WriteString(fmt.Sprintf(" \033[38;5;82m%.1f%%\033[0m\n", pct))
	}

	// Column headers
	colHeader := fmt.Sprintf(" \033[1m\033[38;5;81m%-*s\033[0m │ \033[1m\033[38;5;81m%s\033[0m",
		leftW-1, "Source", "Destination")
	sb.WriteString(colHeader + "\n")
	sb.WriteString(" " + strings.Repeat("─", leftW-1) + "─┼─" + strings.Repeat("─", rightW-1) + "\n")

	destSet := make(map[string]bool, len(ui.destKeys))
	for _, k := range ui.destKeys {
		destSet[k] = true
	}

	// Determine scroll range
	if ui.cursor < ui.scrollOff {
		ui.scrollOff = ui.cursor
	}
	if ui.cursor >= ui.scrollOff+listH {
		ui.scrollOff = ui.cursor - listH + 1
	}
	end := ui.scrollOff + listH
	if end > ui.total {
		end = ui.total
	}

	for i := ui.scrollOff; i < end; i++ {
		if i >= ui.total {
			break
		}
		key := ui.srcKeys[i]
		srcVal := ""
		if i < len(ui.srcValues) {
			srcVal = ui.srcValues[i]
		}
		destVal := ""
		if i < len(ui.destValues) {
			destVal = ui.destValues[i]
		}

		isDest := destSet[key]
		isCursor := i == ui.cursor

		// Color: migrated keys green, pending white, cursor highlighted
		srcColor := "\033[38;5;255m"  // white
		if isDest {
			srcColor = "\033[38;5;82m" // green
		}
		destColor := "\033[38;5;245m" // dim
		if isDest {
			destColor = "\033[38;5;82m" // green
		}

		leftText := key
		if srcVal != "" {
			leftText = key + " " + srcVal
		}
		if len(leftText) > leftW-1 {
			leftText = leftText[:leftW-2]
		}

		rightText := ""
		if isDest {
			if destVal != "" {
				rightText = key + " " + destVal
			} else {
				rightText = key
			}
		} else {
			rightText = "(not migrated)"
		}
		if len(rightText) > rightW-1 {
			rightText = rightText[:rightW-2]
		}

		cursorMark := ""
		if isCursor {
			cursorMark = "\033[7m"
		}
		sb.WriteString(fmt.Sprintf("\033[48;5;236m %s%s%-*s\033[0m │ %s%s\033[0m\n",
			srcColor, cursorMark, leftW-1, leftText, destColor, rightText))
	}

	// Footer
	footerText := " Full: q=quit  |  ↑/↓=scroll  |  Green=migrated  White=pending"
	sb.WriteString("\033[38;5;245m" + strings.Repeat("─", ui.width-1) + "\033[0m")
	sb.WriteString("\033[38;5;245m " + footerText + "\033[0m")

	os.Stdout.WriteString(sb.String())
}

// --- Incremental Migration ---

func runIncMigrate(src, dest *rclient.Client, destDB int) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to enter raw terminal: %v\n", err)
		return
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)
	fmt.Print(migrateHideCursor)
	defer fmt.Print(migrateShowCursor)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH)

	width := 80
	height := 10
	if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
		width = w
		height = h
	}

	fmt.Fprintf(os.Stderr, "\033[H\033[Jincremental migration started... scanning for new keys every 2s\n")
	fmt.Fprintf(os.Stderr, "Press 'q' to stop\n")

	// Get initial key set
	existing := make(map[string]bool)
	for _, k := range scanAllKeys(src) {
		existing[k] = true
	}

	totalMigrated := 0
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		// Check for new keys
		newKeys := scanAllKeys(src)
		found := 0
		for _, k := range newKeys {
			if !existing[k] {
				existing[k] = true
				ttlReply := src.Send(utils.ToCmdLine("TTL", k))
				var ttlMs int64
				if ir, ok := ttlReply.(*protocol.IntReply); ok {
					ttlMs = ir.Code
				}
				migrateKey(dest, src, k, ttlMs)
				found++
			}
		}
		if found > 0 {
			totalMigrated += found
			sb := &strings.Builder{}
			sb.WriteString(fmt.Sprintf("\033[H\033[J Incremental Migration   New keys found: %d   Total migrated: %d\n", found, totalMigrated))
			sb.WriteString(fmt.Sprintf(" \033[38;5;82m- %d new key(s) migrated\033[0m\n", found))
			sb.WriteString("\n \033[38;5;245mPress 'q' to quit\033[0m")
			os.Stdout.WriteString(sb.String())
		}

		// Wait for timer or quit
		select {
		case <-ticker.C:
		case <-sigCh:
			if w, h, err := term.GetSize(int(os.Stdin.Fd())); err == nil {
				width = w
				height = h
			}
		default:
			// Check for 'q'
			var buf [8]byte
			n, _ := os.Stdin.Read(buf[:])
			if n > 0 && (buf[0] == 'q' || buf[0] == 3) {
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func convertToCmdLine(args []string) [][]byte {
	cmd := make([][]byte, len(args))
	for i, a := range args {
		cmd[i] = []byte(a)
	}
	return cmd
}
