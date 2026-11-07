/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package redisinfo 用一个极简的 RESP（REdis Serialization Protocol）客户端
// 读取 Redis 的运行状态。只依赖标准库，不引入 go-redis 之类的重量级依赖，
// 因为 Operator 只需要问一个问题：主节点现在还连着几个从节点。
package redisinfo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Replication 是 INFO replication 里我们关心的字段。
type Replication struct {
	// Role 取值 master 或 slave。
	Role string
	// ConnectedSlaves 是当前与主节点保持连接的从节点数量。
	ConnectedSlaves int32
	// MasterHost / MasterPort 仅在 Role 为 slave 时有意义。
	MasterHost string
	MasterPort int32
	// LinkStatus 是主从链路状态，up 表示同步正常。
	LinkStatus string
}

// Fetch 连接 addr（host:port），可选地执行 AUTH，然后读取 INFO replication。
// timeout 同时作用于建连和读写，避免控制器被慢实例拖死。
func Fetch(ctx context.Context, addr, password string, timeout time.Duration) (*Replication, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("连接 %s 失败: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	rd := bufio.NewReader(conn)
	if password != "" {
		if _, err := roundTrip(conn, rd, "AUTH", password); err != nil {
			return nil, fmt.Errorf("AUTH 失败: %w", err)
		}
	}

	info, err := roundTrip(conn, rd, "INFO", "replication")
	if err != nil {
		return nil, fmt.Errorf("读取 INFO replication 失败: %w", err)
	}
	return Parse(info)
}

// Parse 解析 INFO replication 的返回文本。INFO 的格式是 key:value 逐行排列，
// 组与组之间用 "# Replication" 这样的注释行分隔。
func Parse(info string) (*Replication, error) {
	r := &Replication{}
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "role":
			r.Role = value
		case "connected_slaves":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("connected_slaves 不是整数: %q", value)
			}
			r.ConnectedSlaves = int32(n)
		case "master_host":
			r.MasterHost = value
		case "master_port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("master_port 不是整数: %q", value)
			}
			r.MasterPort = int32(n)
		case "master_link_status":
			r.LinkStatus = value
		}
	}
	if r.Role == "" {
		return nil, fmt.Errorf("响应里没有 role 字段，可能不是 INFO replication 的输出")
	}
	return r, nil
}

// roundTrip 发送一条命令并读回一个 RESP 回复。
// 只实现 README 里够用的四种前缀：+ 简单字符串、- 错误、: 整数、$ 批量字符串。
func roundTrip(conn net.Conn, rd *bufio.Reader, args ...string) (string, error) {
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := conn.Write([]byte(sb.String())); err != nil {
		return "", err
	}
	return readReply(rd)
}

func readReply(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return "", fmt.Errorf("收到空响应")
	}

	switch line[0] {
	case '+', ':':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("服务端返回错误: %s", line[1:])
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", fmt.Errorf("批量字符串长度非法: %q", line)
		}
		if n < 0 {
			// $-1 表示 nil
			return "", nil
		}
		buf := make([]byte, n+2) // 末尾还有 \r\n
		if _, err := io.ReadFull(rd, buf); err != nil {
			return "", err
		}
		return string(buf[:n]), nil
	default:
		return "", fmt.Errorf("不认识的响应类型: %q", line)
	}
}
