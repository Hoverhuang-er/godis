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
	host string
	port int
	auth string
}

func parseCLIFlags() cliFlags {
	f := cliFlags{host: "127.0.0.1", port: 6399}

	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--cli":
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

	stat, _ := os.Stdin.Stat()
	isTerminal := (stat.Mode() & os.ModeCharDevice) != 0

	if isTerminal {
		runInteractive(c, flags)
	} else {
		runBatch(c, os.Stdin, flags)
	}
}

func runInteractive(c *rclient.Client, flags cliFlags) {
	scanner := bufio.NewScanner(os.Stdin)
	prompt := fmt.Sprintf("%s:%d> ", flags.host, flags.port)

	fmt.Print(prompt)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print(prompt)
			continue
		}
		if line == "quit" || line == "exit" || line == "QUIT" {
			break
		}

		args := parseLine(line)
		if len(args) == 0 {
			fmt.Print(prompt)
			continue
		}

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
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "//") {
			continue
		}

		args := parseLine(line)
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(args[0]))
		if cmd == "SUBSCRIBE" || cmd == "PSUBSCRIBE" {
			runPubSub(flags, args)
			continue
		}

		reply := c.Send(args)
		printReply(reply, 0)
	}
}

// runPubSub opens a raw connection for SUBSCRIBE/PSUBSCRIBE mode.
// It sends the subscribe command, then reads and displays push messages.
func runPubSub(flags cliFlags, args [][]byte) {
	addr := net.JoinHostPort(flags.host, strconv.Itoa(flags.port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect: %v\n", err)
		return
	}
	defer conn.Close()

	// Send AUTH if password provided
	if flags.auth != "" {
		authArgs := [][]byte{[]byte("AUTH"), []byte(flags.auth)}
		conn.Write(protocol.MakeMultiBulkReply(authArgs).ToBytes())
		// Read and discard the AUTH response
		resp := make([]byte, 4096)
		n, _ := conn.Read(resp)
		if n > 0 && resp[0] == '-' {
			fmt.Fprintf(os.Stderr, "AUTH failed: %s\n", strings.TrimRight(string(resp[1:n]), "\r\n"))
			return
		}
	}

	// Send the subscribe/psubscribe command via RESP
	cmdPayload := protocol.MakeMultiBulkReply(args).ToBytes()
	conn.Write(cmdPayload)

	// Read and display subscribed/pushed messages
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err.Error() == "EOF" {
				break
			}
			fmt.Fprintf(os.Stderr, "Error: %v\n", payload.Err)
			break
		}
		if payload.Data == nil {
			continue
		}

		data := payload.Data.ToBytes()
		if len(data) == 0 {
			continue
		}

		printPubSubMsg(data)
	}
}

func printPubSubMsg(data []byte) {
	// Expected format: *3\r\n$<n>\r\n<type>\r\n$<n>\r\n<channel>\r\n$<n>\r\n<message>\r\n
	// or subscribe/unsubscribe confirmations.
	parts := splitRESPArray(data)
	if len(parts) < 3 {
		fmt.Println(string(data))
		return
	}

	msgType := string(parts[0])
	channel := string(parts[1])

	switch msgType {
	case "subscribe":
		count := string(parts[2])
		fmt.Printf("1) \"subscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, count)
	case "unsubscribe":
		count := string(parts[2])
		fmt.Printf("1) \"unsubscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, count)
	case "message":
		msg := string(parts[2])
		fmt.Printf("1) \"message\"\n2) \"%s\"\n3) \"%s\"\n", channel, msg)
	case "pmessage":
		pattern := string(parts[0])
		channel = string(parts[1])
		msg := string(parts[2])
		fmt.Printf("1) \"pmessage\"\n2) \"%s\"\n3) \"%s\"\n4) \"%s\"\n", pattern, channel, msg)
	case "psubscribe":
		count := string(parts[2])
		fmt.Printf("1) \"psubscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, count)
	case "punsubscribe":
		count := string(parts[2])
		fmt.Printf("1) \"punsubscribe\"\n2) \"%s\"\n3) (integer) %s\n", channel, count)
	default:
		fmt.Println(string(data))
	}
}

// splitRESPArray splits a RESP array (*3\r\n... ) into element payloads.
func splitRESPArray(data []byte) [][]byte {
	if len(data) == 0 || data[0] != '*' {
		return nil
	}
	var parts [][]byte
	pos := 0
	// skip "*<count>\r\n"
	for pos < len(data) && data[pos] != '\n' {
		pos++
	}
	pos++ // skip \n

	for pos < len(data) {
		if data[pos] == '*' {
			// Nested array - not expected in pubsub
			break
		}
		// Find the element
		start := pos
		if data[pos] == '$' {
			// Bulk string: $<len>\r\n<data>\r\n
			pos++
			for pos < len(data) && data[pos] != '\r' {
				pos++
			}
			pos += 2 // skip \r\n
			// Read the data
			strStart := pos
			for pos < len(data) && !(data[pos] == '\r' && pos+1 < len(data) && data[pos+1] == '\n') {
				pos++
			}
			parts = append(parts, data[strStart:pos])
			pos += 2 // skip \r\n
		} else if data[pos] == ':' {
			// Integer: :<num>\r\n
			pos++
			for pos < len(data) && data[pos] != '\r' {
				pos++
			}
			parts = append(parts, data[start+1:pos])
			pos += 2
		} else if data[pos] == '+' || data[pos] == '-' {
			// Status or error
			pos++
			for pos < len(data) && data[pos] != '\r' {
				pos++
			}
			parts = append(parts, data[start+1:pos])
			pos += 2
		} else {
			break
		}
	}
	return parts
}

func parseLine(line string) [][]byte {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && line[i] == ' ' {
			i++
		}
		if i >= len(line) {
			break
		}

		var arg string
		if line[i] == '"' {
			i++
			for i < len(line) {
				if line[i] == '\\' && i+1 < len(line) {
					arg += string(line[i+1])
					i += 2
				} else if line[i] == '"' {
					i++
					break
				} else {
					arg += string(line[i])
					i++
				}
			}
		} else if line[i] == '\'' {
			i++
			for i < len(line) {
				if line[i] == '\'' {
					i++
					break
				}
				arg += string(line[i])
				i++
			}
		} else {
			for i < len(line) && line[i] != ' ' {
				arg += string(line[i])
				i++
			}
		}
		args = append(args, []byte(arg))
	}
	return args
}

func printReply(r interface{}, depth int) {
	if r == nil {
		fmt.Println("(nil)")
		return
	}

	switch v := r.(type) {
	case redisReply:
		printReplyBytes(v.ToBytes(), depth)
	case *protocol.StatusReply:
		fmt.Println(v.Status)
	case *protocol.IntReply:
		if depth == 0 {
			fmt.Printf("(integer) %d\n", v.Code)
		} else {
			fmt.Printf("%d", v.Code)
		}
	case *protocol.BulkReply:
		if depth == 0 {
			fmt.Printf("\"%s\"\n", string(v.Arg))
		} else {
			fmt.Printf("\"%s\"", string(v.Arg))
		}
	case *protocol.MultiBulkReply:
		if len(v.Args) == 0 {
			fmt.Println("(empty list or set)")
			return
		}
		for i, arg := range v.Args {
			prefix := fmt.Sprintf("%d) ", depth+i+1)
			fmt.Printf("%s\"%s\"\n", prefix, string(arg))
		}
	case *protocol.MultiRawReply:
		for i, reply := range v.Replies {
			if i > 0 {
				fmt.Println()
			}
			prefix := fmt.Sprintf("%d) ", depth+1+i)
			fmt.Print(prefix)
			printReply(reply, depth+1)
		}
	default:
		if rr, ok := r.(redisReply); ok {
			printReplyBytes(rr.ToBytes(), depth)
		} else if rr, ok := r.(fmt.Stringer); ok {
			fmt.Println(rr.String())
		}
	}
}

type redisReply interface {
	ToBytes() []byte
}

func isError(r interface{}) bool {
	if r == nil {
		return false
	}
	if rr, ok := r.(redisReply); ok {
		return len(rr.ToBytes()) > 0 && rr.ToBytes()[0] == '-'
	}
	return false
}

func printReplyBytes(data []byte, depth int) {
	if len(data) == 0 {
		return
	}

	switch data[0] {
	case '+':
		s := string(data[1 : len(data)-2])
		fmt.Println(s)
	case '-':
		s := string(data[1 : len(data)-2])
		fmt.Printf("(error) %s\n", s)
	case ':':
		num := string(data[1 : len(data)-2])
		if depth == 0 {
			fmt.Printf("(integer) %s\n", num)
		} else {
			fmt.Print(num)
		}
	case '$':
		if data[1] == '-' {
			if depth == 0 {
				fmt.Println("(nil)")
			} else {
				fmt.Print("(nil)")
			}
			return
		}
		idx := 2
		for idx < len(data) && data[idx] != '\r' {
			idx++
		}
		contentStart := idx + 2
		contentLen := len(data) - contentStart - 2
		if contentLen >= 0 {
			content := string(data[contentStart : contentStart+contentLen])
			if depth == 0 {
				fmt.Printf("\"%s\"\n", content)
			} else {
				fmt.Printf("\"%s\"", content)
			}
		}
	case '*':
		pos := 1
		for pos < len(data) && data[pos] != '\r' {
			pos++
		}
		pos += 2
		elemIdx := 0
		for pos < len(data) {
			if elemIdx > 0 {
				fmt.Println()
			}
			prefix := fmt.Sprintf("%d) ", depth+elemIdx+1)
			fmt.Print(prefix)

			elemLen, elemBytes := findElement(data[pos:])
			printReplyBytes(elemBytes, depth+1)
			pos += elemLen
			elemIdx++
		}
	default:
		fmt.Println(string(data))
	}
}

func findElement(data []byte) (int, []byte) {
	if len(data) == 0 {
		return 0, nil
	}

	switch data[0] {
	case '+', '-', ':':
		end := 2
		for end < len(data) && !(data[end-2] == '\r' && data[end-1] == '\n') {
			end++
		}
		return end, data[:end]
	case '$':
		idx := 1
		for idx < len(data) && data[idx] != '\r' {
			idx++
		}
		if idx+1 >= len(data) {
			return len(data), data
		}

		lengthStr := string(data[1:idx])
		var length int
		fmt.Sscanf(lengthStr, "%d", &length)

		if length == -1 {
			return idx + 2, data[:idx+2]
		}
		totalLen := idx + 2 + length + 2
		if totalLen > len(data) {
			totalLen = len(data)
		}
		return totalLen, data[:totalLen]
	case '*':
		idx := 1
		for idx < len(data) && data[idx] != '\r' {
			idx++
		}
		pos := idx + 2

		countStr := string(data[1:idx])
		var count int
		fmt.Sscanf(countStr, "%d", &count)

		for i := 0; i < count && pos < len(data); i++ {
			elemLen, _ := findElement(data[pos:])
			pos += elemLen
		}
		return pos, data[:pos]
	default:
		return len(data), data
	}
}

func formatReply(r interface{}) string {
	if r == nil {
		return "(nil)"
	}
	if rr, ok := r.(redisReply); ok {
		data := rr.ToBytes()
		if len(data) == 0 {
			return ""
		}
		switch data[0] {
		case '+', ':':
			return string(data[1 : len(data)-2])
		case '-':
			return string(data[1 : len(data)-2])
		case '$':
			if data[1] == '-' {
				return "(nil)"
			}
			idx := 2
			for idx < len(data) && data[idx] != '\r' {
				idx++
			}
			contentStart := idx + 2
			contentLen := len(data) - contentStart - 2
			if contentLen >= 0 {
				return string(data[contentStart : contentStart+contentLen])
			}
			return ""
		default:
			return string(data)
		}
	}
	return fmt.Sprintf("%v", r)
}
