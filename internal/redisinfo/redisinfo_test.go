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

package redisinfo

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

const sampleInfo = "# Replication\r\nrole:master\r\nconnected_slaves:2\r\nslave0:ip=10.0.0.2,port=6379,state=online,offset=1234,lag=0\r\n" +
	"slave1:ip=10.0.0.3,port=6379,state=online,offset=1234,lag=0\r\nmaster_replid:abc\r\nmaster_repl_offset:1234\r\n"

func TestParseMaster(t *testing.T) {
	got, err := Parse(sampleInfo)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got.Role != "master" {
		t.Errorf("role = %q, 期望 master", got.Role)
	}
	if got.ConnectedSlaves != 2 {
		t.Errorf("connected_slaves = %d, 期望 2", got.ConnectedSlaves)
	}
}

func TestParseReplica(t *testing.T) {
	info := "# Replication\r\nrole:slave\r\nmaster_host:redis-0.redis-headless.default.svc.cluster.local\r\n" +
		"master_port:6379\r\nmaster_link_status:up\r\nconnected_slaves:0\r\n"
	got, err := Parse(info)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if got.Role != "slave" || got.MasterPort != 6379 || got.LinkStatus != "up" {
		t.Errorf("解析结果不对: %+v", got)
	}
}

func TestParseMissingRole(t *testing.T) {
	if _, err := Parse("total_connections_received:1\r\n"); err == nil {
		t.Fatal("缺少 role 字段时应该报错")
	}
}

// TestFetchWithAuth 起一个假 Redis，验证客户端会先 AUTH 再发 INFO，
// 并把批量字符串正确拼回来。不依赖真实 Redis，可在任何机器上跑。
func TestFetchWithAuth(t *testing.T) {
	addr, stop := startFakeRedis(t, "s3cret", sampleInfo)
	defer stop()

	got, err := Fetch(context.Background(), addr, "s3cret", 2*time.Second)
	if err != nil {
		t.Fatalf("Fetch 失败: %v", err)
	}
	if got.Role != "master" || got.ConnectedSlaves != 2 {
		t.Fatalf("结果不对: %+v", got)
	}
}

func TestFetchWrongPassword(t *testing.T) {
	addr, stop := startFakeRedis(t, "s3cret", sampleInfo)
	defer stop()

	if _, err := Fetch(context.Background(), addr, "wrong", 2*time.Second); err == nil {
		t.Fatal("密码错误时应该返回 error")
	}
}

// startFakeRedis 返回一个只认 AUTH / INFO 的最小 RESP 服务端。
func startFakeRedis(t *testing.T, password, info string) (addr string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("监听失败: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleFakeConn(conn, password, info)
		}
	}()

	return ln.Addr().String(), func() { _ = ln.Close() }
}

func handleFakeConn(conn net.Conn, password, info string) {
	defer func() { _ = conn.Close() }()
	rd := bufio.NewReader(conn)

	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		switch strings.ToUpper(args[0]) {
		case "AUTH":
			if len(args) != 2 || args[1] != password {
				_, _ = conn.Write([]byte("-WRONGPASS invalid password\r\n"))
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "INFO":
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(info), info)
		default:
			_, _ = conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

// readCommand 读一条完整的 RESP 数组命令。
func readCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("期望数组开头，实际是 %q", line)
	}

	var n int
	if _, err := fmt.Sscanf(line, "*%d", &n); err != nil {
		return nil, err
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		header, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimRight(header, "\r\n")

		var size int
		if _, err := fmt.Sscanf(header, "$%d", &size); err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}
