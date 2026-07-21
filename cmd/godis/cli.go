package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/Hoverhuang-er/godis/internal/lib/utils"
	rclient "github.com/Hoverhuang-er/godis/internal/redis/client"
	"github.com/Hoverhuang-er/godis/internal/redis/parser"
	"github.com/Hoverhuang-er/godis/internal/redis/protocol"
)

type cliFlags struct {
	host        string
	port        int
	auth        string
	entraTenant string
	entraApp    string
}

func parseCLIFlags() cliFlags {
	f := cliFlags{host: "127.0.0.1", port: 6399}
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--cli":
		case "-u":
			if i+1 < len(args) {
				i++
				// parse redis://user:pass@host:port
				raw := args[i]
				if strings.HasPrefix(raw, "redis://") {
					rest := raw[8:]
					if atIdx := strings.LastIndex(rest, "@"); atIdx >= 0 {
						userInfo := rest[:atIdx]
						rest = rest[atIdx+1:]
						if colonIdx := strings.Index(userInfo, ":"); colonIdx >= 0 {
							f.auth = userInfo[colonIdx+1:]
						}
					}
					if colonIdx := strings.LastIndex(rest, ":"); colonIdx >= 0 {
						if p, err := strconv.Atoi(rest[colonIdx+1:]); err == nil {
							f.port = p
						}
						f.host = rest[:colonIdx]
					} else {
						f.host = rest
					}
				}
			}
		case "-h":
			if i+1 < len(args) {
				i++
				f.host = args[i]
			}
		case "-p":
			if i+1 < len(args) {
				i++
				port, err := strconv.Atoi(args[i])
				if err == nil {
					f.port = port
				}
			}
		case "-a":
			if i+1 < len(args) {
				i++
				f.auth = args[i]
			}
		case "--entra-tenant":
			if i+1 < len(args) {
				i++
				f.entraTenant = args[i]
			}
		case "--entra-app":
			if i+1 < len(args) {
				i++
				f.entraApp = args[i]
			}
		}
	}
	return f
}

func runCLI() {
	flags := parseCLIFlags()

	addr := net.JoinHostPort(flags.host, strconv.Itoa(flags.port))
	c, err := rclient.MakeClient(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to godis at %s: %v\n", addr, err)
		os.Exit(1)
	}
	c.Start()
	defer c.Close()

	if flags.auth != "" {
		reply := c.Send(utils.ToCmdLine("AUTH", flags.auth))
		if isError(reply) {
			fmt.Fprintf(os.Stderr, "AUTH failed: %s\n", formatReply(reply))
			os.Exit(1)
		}
	}

	runCLITUI(c, flags)
}

func runInteractive(c *rclient.Client, flags cliFlags) {
	scanner := bufio.NewScanner(os.Stdin)
	prompt := fmt.Sprintf("%s:%d> ", flags.host, flags.port)

	fmt.Print(prompt)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" { fmt.Print(prompt); continue }
		if line == "quit" || line == "exit" || line == "QUIT" { break }

		args := parseLine(line)
		if len(args) == 0 { fmt.Print(prompt); continue }

		cmd := strings.ToUpper(string(args[0]))
		if cmd == "SUBSCRIBE" || cmd == "PSUBSCRIBE" {
			runPubSub(flags, args)
			fmt.Print(prompt)
			continue
		}
		reply := c.Send(args)
		printReply(reply, 0)
		fmt.Print(prompt)
	}
	fmt.Println()
}

func runBatch(c *rclient.Client, r io.Reader, flags cliFlags) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") { continue }
		args := parseLine(line)
		if len(args) == 0 { continue }
		cmd := strings.ToUpper(string(args[0]))
		if cmd == "SUBSCRIBE" || cmd == "PSUBSCRIBE" {
			runPubSub(flags, args); continue
		}
		reply := c.Send(args)
		printReply(reply, 0)
	}
}

func runPubSub(flags cliFlags, args [][]byte) {
	addr := net.JoinHostPort(flags.host, strconv.Itoa(flags.port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect: %v\n", err)
		return
	}
	defer conn.Close()

	if flags.auth != "" {
		authArgs := [][]byte{[]byte("AUTH"), []byte(flags.auth)}
		conn.Write(protocol.MakeMultiBulkReply(authArgs).ToBytes())
		resp := make([]byte, 4096)
		n, _ := conn.Read(resp)
		if n > 0 && resp[0] == '-' {
			fmt.Fprintf(os.Stderr, "AUTH failed: %s\n", strings.TrimRight(string(resp[1:n]), "\r\n"))
			return
		}
	}
	cmdPayload := protocol.MakeMultiBulkReply(args).ToBytes()
	conn.Write(cmdPayload)

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err.Error() == "EOF" { break }
			fmt.Fprintf(os.Stderr, "Error: %v\n", payload.Err)
			break
		}
		if payload.Data == nil { continue }
		data := payload.Data.ToBytes()
		if len(data) == 0 { continue }
		printPubSubMsg(data)
	}
}

func printPubSubMsg(data []byte) {
	parts := splitRESPArray(data)
	if len(parts) < 3 { fmt.Println(string(data)); return }
	msgType := string(parts[0])
	channel := string(parts[1])
	switch msgType {
	case "subscribe":
		fmt.Printf("1) \"subscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, string(parts[2]))
	case "unsubscribe":
		fmt.Printf("1) \"unsubscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, string(parts[2]))
	case "message":
		fmt.Printf("1) \"message\"\n2) \"%s\"\n3) \"%s\"\n", channel, string(parts[2]))
	case "pmessage":
		fmt.Printf("1) \"pmessage\"\n2) \"%s\"\n3) \"%s\"\n4) \"%s\"\n", string(parts[0]), channel, string(parts[2]))
	case "psubscribe":
		fmt.Printf("1) \"psubscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, string(parts[2]))
	default:
		fmt.Println(string(data))
	}
}

func splitRESPArray(data []byte) [][]byte {
	if len(data) == 0 || data[0] != '*' { return nil }
	var parts [][]byte
	pos := 0
	for pos < len(data) && data[pos] != '\n' { pos++ }
	pos++
	for pos < len(data) {
		if data[pos] == '*' { break }
		if data[pos] == '$' {
			pos++
			for pos < len(data) && data[pos] != '\r' { pos++ }
			pos += 2
			strStart := pos
			for pos < len(data) && !(data[pos] == '\r' && pos+1 < len(data) && data[pos+1] == '\n') { pos++ }
			parts = append(parts, data[strStart:pos])
			pos += 2
		} else if data[pos] == ':' {
			pos++
			start := pos
			for pos < len(data) && data[pos] != '\r' { pos++ }
			parts = append(parts, data[start:pos])
			pos += 2
		} else if data[pos] == '+' || data[pos] == '-' {
			pos++
			start := pos
			for pos < len(data) && data[pos] != '\r' { pos++ }
			parts = append(parts, data[start:pos])
			pos += 2
		} else { break }
	}
	return parts
}

func parseLine(line string) [][]byte {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' { i++ }
		if i >= len(line) { break }
		var arg string
		if line[i] == '"' {
			i++
			for i < len(line) {
				if line[i] == '\\' && i+1 < len(line) { arg += string(line[i+1]); i += 2 } else if line[i] == '"' { i++; break } else { arg += string(line[i]); i++ }
			}
		} else if line[i] == '\'' {
			i++
			for i < len(line) { if line[i] == '\'' { i++; break }; arg += string(line[i]); i++ }
		} else {
			for i < len(line) && line[i] != ' ' { arg += string(line[i]); i++ }
		}
		args = append(args, []byte(arg))
	}
	return args
}

func printReply(r interface{}, depth int) {
	if r == nil { fmt.Println("(nil)"); return }
	switch v := r.(type) {
	case redisReply:
		printReplyBytes(v.ToBytes(), depth)
	case *protocol.StatusReply:
		fmt.Println(v.Status)
	case *protocol.IntReply:
		if depth == 0 { fmt.Printf("(integer) %d\n", v.Code) } else { fmt.Printf("%d", v.Code) }
	case *protocol.BulkReply:
		if depth == 0 { fmt.Printf("\"%s\"\n", string(v.Arg)) } else { fmt.Printf("\"%s\"", string(v.Arg)) }
	case *protocol.MultiBulkReply:
		if len(v.Args) == 0 { fmt.Println("(empty list or set)"); return }
		for i, arg := range v.Args { fmt.Printf("%d) \"%s\"\n", depth+i+1, string(arg)) }
	case *protocol.MultiRawReply:
		for i, reply := range v.Replies {
			if i > 0 { fmt.Println() }
			fmt.Printf("%d) ", depth+1+i)
			printReply(reply, depth+1)
		}
	default:
		if rr, ok := r.(redisReply); ok { printReplyBytes(rr.ToBytes(), depth) } else if rr, ok := r.(fmt.Stringer); ok { fmt.Println(rr.String()) }
	}
}

type redisReply interface{ ToBytes() []byte }

func isError(r interface{}) bool {
	if r == nil { return false }
	if rr, ok := r.(redisReply); ok { return len(rr.ToBytes()) > 0 && rr.ToBytes()[0] == '-' }
	return false
}

func printReplyBytes(data []byte, depth int) {
	if len(data) == 0 { return }
	switch data[0] {
	case '+':
		fmt.Println(string(data[1 : len(data)-2]))
	case '-':
		fmt.Printf("(error) %s\n", string(data[1:len(data)-2]))
	case ':':
		n := string(data[1 : len(data)-2])
		if depth == 0 { fmt.Printf("(integer) %s\n", n) } else { fmt.Print(n) }
	case '$':
		if data[1] == '-' { fmt.Println("(nil)"); return }
		idx := 2; for idx < len(data) && data[idx] != '\r' { idx++ }
		cs := idx + 2; cl := len(data) - cs - 2
		if cl >= 0 { fmt.Printf("\"%s\"\n", string(data[cs:cs+cl])) }
	case '*':
		pos := 1; for pos < len(data) && data[pos] != '\r' { pos++ }; pos += 2
		e := 0
		for pos < len(data) {
			if e > 0 { fmt.Println() }
			fmt.Printf("%d) ", depth+e+1)
			el, eb := findElement(data[pos:])
			printReplyBytes(eb, depth+1)
			pos += el; e++
		}
	default: fmt.Println(string(data))
	}
}

func findElement(data []byte) (int, []byte) {
	if len(data) == 0 { return 0, nil }
	switch data[0] {
	case '+', '-', ':':
		end := 2; for end < len(data) && !(data[end-2] == '\r' && data[end-1] == '\n') { end++ }
		return end, data[:end]
	case '$':
		idx := 1; for idx < len(data) && data[idx] != '\r' { idx++ }
		if idx+1 >= len(data) { return len(data), data }
		var l int; fmt.Sscanf(string(data[1:idx]), "%d", &l)
		if l == -1 { return idx + 2, data[:idx+2] }
		tl := idx + 2 + l + 2; if tl > len(data) { tl = len(data) }
		return tl, data[:tl]
	case '*':
		idx := 1; for idx < len(data) && data[idx] != '\r' { idx++ }; pos := idx + 2
		var c int; fmt.Sscanf(string(data[1:idx]), "%d", &c)
		for i := 0; i < c && pos < len(data); i++ { el, _ := findElement(data[pos:]); pos += el }
		return pos, data[:pos]
	default: return len(data), data
	}
}

func formatReply(r interface{}) string {
	if r == nil { return "(nil)" }
	if rr, ok := r.(redisReply); ok {
		d := rr.ToBytes(); if len(d) == 0 { return "" }
		switch d[0] {
		case '+', ':': return string(d[1 : len(d)-2])
		case '-': return string(d[1 : len(d)-2])
		case '$':
			if d[1] == '-' { return "(nil)" }
			idx := 2; for idx < len(d) && d[idx] != '\r' { idx++ }
			cs := idx + 2; cl := len(d) - cs - 2
			if cl >= 0 { return string(d[cs : cs+cl]) }
			return ""
		default: return string(d)
		}
	}
	return fmt.Sprintf("%v", r)
}
