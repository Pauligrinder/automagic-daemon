package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/godbus/dbus/v5"
)

const (
	dbusReconnectInitial = time.Second
	dbusReconnectMax     = 30 * time.Second
)

func (e *Engine) connectToDBus(busAddr string) (*dbus.Conn, error) {
	if busAddr == "system" {
		// Owned connection so callers can Close and reconnect safely.
		return dbus.ConnectSystemBus()
	} else if busAddr == "session" || busAddr == "" {
		if e.UID != "" {
			busAddr = fmt.Sprintf("unix:path=/run/user/%s/dbus/user_bus_socket", e.UID)
		} else {
			return dbus.ConnectSessionBus()
		}
	}

	conn, err := dbus.Dial(busAddr)
	if err != nil {
		return nil, err
	}

	if err = conn.Auth([]dbus.Auth{dbus.AuthExternal(e.UID)}); err != nil {
		conn.Close()
		return nil, err
	}

	if err = conn.Hello(); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func nextDBusBackoff(current time.Duration) time.Duration {
	next := current * 2
	if next > dbusReconnectMax {
		return dbusReconnectMax
	}
	if next < dbusReconnectInitial {
		return dbusReconnectInitial
	}
	return next
}

// startDBusConnection keeps a bus connection alive for D-Bus signal triggers.
// Sailfish often starts automagicd before the user session bus exists; without
// retry the daemon would permanently miss session signals until a manual reload.
func (e *Engine) startDBusConnection(ctx context.Context, busAddr string, triggers []DataSource) {
	backoff := dbusReconnectInitial

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := e.connectToDBus(busAddr)
		if err != nil {
			log.Printf("startDBusConnection - failed to connect to bus '%s': %v; retrying in %s", busAddr, err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = nextDBusBackoff(backoff)
			continue
		}
		backoff = dbusReconnectInitial

		lost := e.listenDBusConnection(ctx, conn, busAddr, triggers)
		_ = conn.Close()

		if ctx.Err() != nil {
			log.Printf("startDBusConnection - shutting down connection to bus: %s", busAddr)
			return
		}
		if !lost {
			return
		}

		log.Printf("startDBusConnection - connection to bus '%s' lost; reconnecting in %s", busAddr, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = nextDBusBackoff(backoff)
	}
}

// listenDBusConnection registers matches and reads signals until the context is
// cancelled or the connection drops. Returns true when the caller should reconnect.
func (e *Engine) listenDBusConnection(ctx context.Context, conn *dbus.Conn, busAddr string, triggers []DataSource) bool {
	for _, t := range triggers {
		if t.Interface == "" {
			continue
		}
		match := fmt.Sprintf("type='signal',interface='%s'", t.Interface)
		if t.Path != "" {
			match += fmt.Sprintf(",path='%s'", t.Path)
		}
		if t.Signal != "" {
			match += fmt.Sprintf(",member='%s'", t.Signal)
		}

		log.Printf("[%s] Registering match: %s", busAddr, match)
		err := conn.BusObject().Call("org.freedesktop.DBus.AddMatch", 0, match).Store()
		if err != nil {
			log.Printf("startDBusConnection - failed to add match for %s: %v", t.Interface, err)
		}
	}

	c := make(chan *dbus.Signal, 32)
	conn.Signal(c)
	defer conn.RemoveSignal(c)

	log.Printf("startDBusConnection - listening on bus: %s", busAddr)

	for {
		select {
		case <-ctx.Done():
			return false
		case v, ok := <-c:
			if !ok || v == nil {
				log.Printf("startDBusConnection - bus '%s' signal stream ended", busAddr)
				return true
			}
			e.handleDBusSignal(v, triggers)
		}
	}
}
