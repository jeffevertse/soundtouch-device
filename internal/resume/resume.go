// Package resume re-plays the last station when the speaker powers on, using the
// SoundTouch WebSocket event stream (via the gesellix client).
package resume

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gesellix/bose-soundtouch/pkg/client"
	"github.com/gesellix/bose-soundtouch/pkg/models"
)

// resumeDebounce ignores repeat power-on triggers (the speaker can emit a couple
// of STANDBY→on transitions in quick succession during wake-up).
const resumeDebounce = 10 * time.Second

// Watcher detects STANDBY→on transitions and invokes play().
type Watcher struct {
	client     *client.Client
	play       func()
	mu         sync.Mutex
	prev       string
	lastResume time.Time
}

func New(c *client.Client, play func()) *Watcher {
	return &Watcher{client: c, play: play}
}

// Start connects the WebSocket and watches for power-on. The gesellix client
// auto-reconnects, so this returns after the initial connect.
func (w *Watcher) Start() error {
	ws := w.client.NewWebSocketClient(client.DefaultWebSocketConfig())
	ws.OnNowPlaying(func(ev *models.NowPlayingUpdatedEvent) {
		src := strings.ToUpper(strings.TrimSpace(ev.NowPlaying.Source))
		w.mu.Lock()
		prev := w.prev
		w.prev = src
		powerOn := prev == "STANDBY" && src != "" && src != "STANDBY"
		fire := powerOn && time.Since(w.lastResume) >= resumeDebounce
		if fire {
			w.lastResume = time.Now()
		}
		w.mu.Unlock()
		switch {
		case fire:
			log.Printf("[resume] power-on detected (%s -> %s) — resuming", prev, src)
			go w.play()
		case powerOn:
			log.Printf("[resume] power-on (%s -> %s) ignored (debounced)", prev, src)
		}
	})
	return ws.Connect()
}
