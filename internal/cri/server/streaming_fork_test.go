/*
   Copyright The containerd Authors.

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

package server

// fivetime fork: these guard two changes carried in vendor/ (k8s.io/streaming
// wsstream and k8s.io/cri-streaming remotecommand). Re-vendoring from
// upstream drops them silently; these tests are what notices.

import (
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/websocket"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	streaming "k8s.io/cri-streaming/pkg/streaming"
	remotecommand "k8s.io/cri-streaming/pkg/streaming/remotecommand"
)

type forkStreamRuntime struct {
	exec func(ctx context.Context, in io.Reader, out io.WriteCloser) error
}

func (r *forkStreamRuntime) Exec(ctx context.Context, _ string, _ []string, in io.Reader, out, _ io.WriteCloser, _ bool, _ <-chan remotecommand.TerminalSize) error {
	return r.exec(ctx, in, out)
}

func (r *forkStreamRuntime) Attach(context.Context, string, io.Reader, io.WriteCloser, io.WriteCloser, bool, <-chan remotecommand.TerminalSize) error {
	return nil
}

func (r *forkStreamRuntime) PortForward(context.Context, string, int32, io.ReadWriteCloser) error {
	return nil
}

// dialExec starts a stream server around rt and opens an exec on it over a
// websocket speaking protocol.
func dialExec(t *testing.T, rt streaming.Runtime, protocol string) *websocket.Conn {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil)
	base, err := url.Parse("http://" + srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	config := streaming.DefaultConfig
	config.Addr = srv.Listener.Addr().String()
	config.BaseURL = base
	ss, err := streaming.NewServer(config, rt)
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = ss
	srv.Start()
	t.Cleanup(srv.Close)

	resp, err := ss.GetExec(&runtimeapi.ExecRequest{
		ContainerId: "c", Cmd: []string{"cat"}, Stdin: true, Stdout: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	wsURL := "ws" + strings.TrimPrefix(resp.Url, "http")
	wsConfig, err := websocket.NewConfig(wsURL, "http://localhost")
	if err != nil {
		t.Fatal(err)
	}
	wsConfig.Protocol = []string{protocol}
	ws, err := websocket.DialConfig(wsConfig)
	if err != nil {
		t.Fatalf("dialing %s: %v", protocol, err)
	}
	return ws
}

// A v5 client ends stdin with [255, 0] and still reads the command's output.
func TestForkExecWebsocketV5EndsStdin(t *testing.T) {
	rt := &forkStreamRuntime{exec: func(_ context.Context, in io.Reader, out io.WriteCloser) error {
		data, err := io.ReadAll(in) // returns only once stdin is closed
		if err != nil {
			return err
		}
		_, err = out.Write(append([]byte("got:"), data...))
		return err
	}}
	ws := dialExec(t, rt, "v5.channel.k8s.io")
	defer ws.Close()
	if ws.Config().Protocol[0] != "v5.channel.k8s.io" {
		t.Fatalf("negotiated %q, want v5.channel.k8s.io", ws.Config().Protocol)
	}

	for _, frame := range [][]byte{{0, 'h', 'i'}, {255, 0}} {
		if err := websocket.Message.Send(ws, frame); err != nil {
			t.Fatal(err)
		}
	}

	ws.SetDeadline(time.Now().Add(10 * time.Second))
	var stdout []byte
	for {
		var frame []byte
		if err := websocket.Message.Receive(ws, &frame); err != nil {
			t.Fatalf("before the exit status: %v (stdout so far %q)", err, stdout)
		}
		if len(frame) == 0 {
			continue
		}
		switch frame[0] {
		case 1:
			stdout = append(stdout, frame[1:]...)
		case 3:
			if !strings.Contains(string(frame[1:]), `"Success"`) {
				t.Fatalf("status %s", frame[1:])
			}
			if string(stdout) != "got:hi" {
				t.Fatalf("stdout %q, want %q", stdout, "got:hi")
			}
			return
		}
	}
}

// A websocket client that goes away ends the exec, as SPDY's does: the
// runtime kills the process when the context it was given ends.
func TestForkExecWebsocketDisconnectCancels(t *testing.T) {
	for _, protocol := range []string{"v4.channel.k8s.io", "v5.channel.k8s.io"} {
		t.Run(protocol, func(t *testing.T) {
			cancelled := make(chan struct{})
			rt := &forkStreamRuntime{exec: func(ctx context.Context, _ io.Reader, _ io.WriteCloser) error {
				select {
				case <-ctx.Done():
					close(cancelled)
				case <-time.After(30 * time.Second):
				}
				return nil
			}}
			ws := dialExec(t, rt, protocol)
			// The server's first frame says the streams are up.
			var frame []byte
			ws.SetDeadline(time.Now().Add(10 * time.Second))
			if err := websocket.Message.Receive(ws, &frame); err != nil {
				t.Fatal(err)
			}
			ws.Close()

			select {
			case <-cancelled:
			case <-time.After(10 * time.Second):
				t.Fatal("the exec's context was not cancelled after its client disconnected")
			}
		})
	}
}
