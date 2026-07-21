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
	"github.com/Hoverhuang-er/godis/internal/redis/client"
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
			// already handled, skip
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
	c, err := client.MakeClient(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not connect to godis at %s: %v\n", addr, err)
		os.Exit(1)
	}
	c.Start()
	defer c.Close()

	// Authenticate if password provided
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
		runInteractive(c, flags.host, flags.port)
	} else {
		runBatch(c, os.Stdin)
	}
}

func runInteractive(c *client.Client, host string, port int) {
	scanner := bufio.NewScanner(os.Stdin)
	prompt := fmt.Sprintf("%s:%d> ", host, port)

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

		reply := c.Send(args)
		printReply(reply, 0)
		fmt.Print(prompt)
	}
	fmt.Println()
}

func runBatch(c *client.Client, r io.Reader) {
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

		reply := c.Send(args)
		printReply(reply, 0)
	}
}

func parseLine(line string) [][]byte {
	// Simple space-separated argument parsing
	// Supports double-quoted strings with escaped characters
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
			// Quoted string
			i++ // skip opening quote
			for i < len(line) {
				if line[i] == '\\' && i+1 < len(line) {
					arg += string(line[i+1])
					i += 2
				} else if line[i] == '"' {
					i++ // skip closing quote
					break
				} else {
					arg += string(line[i])
					i++
				}
			}
		} else if line[i] == '\'' {
			// Single-quoted string
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
		fmt.Printf("%d) \"%s\"\n", depth+1, string(v.Args[0]))
		for i, arg := range v.Args[1:] {
			fmt.Printf("%d) \"%s\"\n", depth+2+i, string(arg))
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
		} else {
			fmt.Println(string(r.(redisReply).ToBytes()))
		}
	}
}

// redisReply matches the interface{ToBytes()[]byte} signature
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
		// Status reply: +OK\r\n
		s := string(data[1 : len(data)-2])
		fmt.Println(s)
	case '-':
		// Error reply: -ERR message\r\n
		s := string(data[1 : len(data)-2])
		fmt.Printf("(error) %s\n", s)
	case ':':
		// Integer reply: :1\r\n
		num := string(data[1 : len(data)-2])
		if depth == 0 {
			fmt.Printf("(integer) %s\n", num)
		} else {
			fmt.Print(num)
		}
	case '$':
		// Bulk string: $-1\r\n or $5\r\nhello\r\n
		if data[1] == '-' {
			// Null bulk
			if depth == 0 {
				fmt.Println("(nil)")
			} else {
				fmt.Print("(nil)")
			}
			return
		}
		// Find the \r\n after the length
		idx := 2
		for idx < len(data) && data[idx] != '\r' {
			idx++
		}
		// Content starts after the first \r\n
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
		// Array
		// Parse the count
		endIdx := 1
		for endIdx < len(data) && data[endIdx] != '\r' {
			endIdx++
		}
		// countStr := string(data[1:endIdx])
		// Parse each element recursively
		pos := endIdx + 2
		elemIdx := 0
		for pos < len(data) {
			if elemIdx > 0 {
				fmt.Println()
			}
			prefix := fmt.Sprintf("%d) ", depth+elemIdx+1)
			fmt.Print(prefix)

			// Find the end of this element and recurse
			elemLen, elemBytes := findElement(data[pos:])
			printReplyBytes(elemBytes, depth+1)

			pos += elemLen
			elemIdx++
		}
	default:
		fmt.Println(string(data))
	}
}

// findElement finds the next complete RESP element starting at the given position
func findElement(data []byte) (int, []byte) {
	if len(data) == 0 {
		return 0, nil
	}

	switch data[0] {
	case '+', '-', ':':
		// Simple replies end with \r\n
		end := 2
		for end < len(data) && !(data[end-2] == '\r' && data[end-1] == '\n') {
			end++
		}
		return end, data[:end]
	case '$':
		// Bulk string
		// Find \r\n after length
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
			// Null bulk: $-1\r\n
			return idx + 2, data[:idx+2]
		}

		// $<length>\r\n<data>\r\n
		totalLen := idx + 2 + length + 2
		if totalLen > len(data) {
			totalLen = len(data)
		}
		return totalLen, data[:totalLen]
	case '*':
		// Array
		// Find \r\n after count
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
		// Unknown, return as-is
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
