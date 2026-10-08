package mcp

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

// stdioRW wraps a child process's stdin/stdout as a bidirectional transport.
type stdioRW struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

// Read reads the child process's stdout.
func (s *stdioRW) Read(p []byte) (int, error) { return s.stdout.Read(p) }

// Write writes to the child process's stdin.
func (s *stdioRW) Write(p []byte) (int, error) { return s.stdin.Write(p) }

// Close closes the pipes and terminates the child process.
//
// Kill unblocks concurrent Writes stuck on the pipe (broken-pipe EPIPE),
// and Wait reaps the corpse to prevent Unix zombie processes from piling
// up.
func (s *stdioRW) Close() error {
	s.stdin.Close()
	s.stdout.Close()
	if err := s.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = s.cmd.Wait()
	return nil
}

// ConnectStdio launches a child-process MCP server and completes the
// handshake.
// name: the server name, for log attribution
// command: the executable
// args: command-line arguments
// returns: a client that has completed the handshake
func ConnectStdio(ctx context.Context, name, command string, args ...string) (*Client, error) {
	cmd := exec.Command(command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp %s stdin: %w", name, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("mcp %s stdout: %w", name, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("mcp %s start: %w", name, err)
	}

	c := NewClient(&stdioRW{cmd: cmd, stdin: stdin, stdout: stdout}, name)
	if err := c.Connect(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}
