package mcp

import (
	"context"
	"fmt"
	"io"
	"os/exec"
)

// stdioRW 子进程 stdin/stdout 包装为双向传输
type stdioRW struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
}

// Read 读子进程 stdout
func (s *stdioRW) Read(p []byte) (int, error) { return s.stdout.Read(p) }

// Write 写子进程 stdin
func (s *stdioRW) Write(p []byte) (int, error) { return s.stdin.Write(p) }

// Close 关闭管道并终止子进程
//
// Kill 会解除阻塞在管道写上的并发 Write（管道破裂 EPIPE），
// Wait 收尸防止 Unix 僵尸进程堆积
func (s *stdioRW) Close() error {
	s.stdin.Close()
	s.stdout.Close()
	if err := s.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = s.cmd.Wait()
	return nil
}

// ConnectStdio 启动子进程 MCP 服务器并完成握手
// name: 服务器名，日志定位用
// command: 可执行文件
// args: 命令行参数
// returns: 已完成握手的客户端
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
