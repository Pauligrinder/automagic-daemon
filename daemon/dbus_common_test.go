package main

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestNextDBusBackoff(t *testing.T) {
	if got := nextDBusBackoff(dbusReconnectInitial); got != 2*time.Second {
		t.Fatalf("first step: got %v", got)
	}
	if got := nextDBusBackoff(20 * time.Second); got != dbusReconnectMax {
		t.Fatalf("cap: got %v", got)
	}
}

func TestStartDBusConnectionRetriesUntilCancelled(t *testing.T) {
	e := NewEngine()
	e.UID = "0"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		e.startDBusConnection(ctx, "unix:path=/tmp/automagic-missing-dbus-socket", nil)
		close(done)
	}()

	// Previously this returned immediately on the first dial failure.
	time.Sleep(1500 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("startDBusConnection returned after connect failure instead of retrying")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startDBusConnection did not stop after cancel")
	}
}

func TestStartDBusConnectionReconnectingAfterBusExit(t *testing.T) {
	if _, err := exec.LookPath("dbus-daemon"); err != nil {
		t.Skip("dbus-daemon not installed")
	}

	dir := t.TempDir()
	sock := filepath.Join(dir, "bus")
	addr := "unix:path=" + sock

	cmd := exec.Command("dbus-daemon", "--session", "--nofork", "--address="+addr)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("start dbus-daemon: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	// Wait until the socket appears.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("dbus socket never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}

	e := NewEngine()
	e.UID = strconv.Itoa(os.Getuid())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})

	go func() {
		e.startDBusConnection(ctx, addr, nil)
		close(done)
	}()

	// Allow the first successful listen to start.
	time.Sleep(500 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("listener exited while dbus-daemon was still running")
	default:
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill dbus-daemon: %v", err)
	}
	_ = cmd.Wait()

	// Restart the bus; the listener should reconnect instead of staying dead.
	cmd = exec.Command("dbus-daemon", "--session", "--nofork", "--address="+addr)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatalf("restart dbus-daemon: %v", err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted dbus socket never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	select {
	case <-done:
		t.Fatal("listener exited after bus restart instead of reconnecting")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("listener did not stop after cancel")
	}
}
