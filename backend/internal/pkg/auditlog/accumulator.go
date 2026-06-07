package auditlog

import (
	"context"
	"strings"
	"sync"
)

// Accumulator is a lightweight response body collector that rides along in
// the request context. Service-layer handlers call its methods to accumulate
// response data (SSE lines, WebSocket frames, or non-streaming bodies).
// After the request completes, the handler retrieves the accumulated body and
// includes it in the audit Record.
//
// Accumulator is safe for concurrent use: SSE reader goroutines call
// AppendSSELine / AppendWSMessage, and the main goroutine calls ResponseBody
// only after the stream ends.
type Accumulator struct {
	mu        sync.Mutex
	buf       strings.Builder
	maxBytes  int
	truncated bool
	enabled   bool
}

// NewAccumulator creates an Accumulator. If enabled is false all methods are no-ops.
// maxBytes caps the accumulated body size (0 = unlimited).
func NewAccumulator(enabled bool, maxBytes int) *Accumulator {
	return &Accumulator{
		enabled:  enabled,
		maxBytes: maxBytes,
	}
}

// AppendSSELine appends a single SSE data line to the response buffer.
// Called from streaming handlers for each "data: ..." line.
func (a *Accumulator) AppendSSELine(line string) {
	if a == nil || !a.enabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.truncated {
		return
	}
	if a.maxBytes > 0 && a.buf.Len()+len(line)+1 > a.maxBytes {
		a.truncated = true
		return
	}
	a.buf.WriteString(line)
	a.buf.WriteByte('\n')
}

// AppendWSMessage appends a WebSocket frame payload to the response buffer.
func (a *Accumulator) AppendWSMessage(payload []byte) {
	if a == nil || !a.enabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.truncated {
		return
	}
	if a.maxBytes > 0 && a.buf.Len()+len(payload)+1 > a.maxBytes {
		a.truncated = true
		return
	}
	a.buf.Write(payload)
	a.buf.WriteByte('\n')
}

// SetNonStreamingBody sets the complete response body for non-streaming requests.
func (a *Accumulator) SetNonStreamingBody(body []byte) {
	if a == nil || !a.enabled {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.maxBytes > 0 && len(body) > a.maxBytes {
		a.buf.Write(body[:a.maxBytes])
		a.truncated = true
		return
	}
	a.buf.Write(body)
}

// ResponseBody returns the accumulated response body string.
// Should be called after the stream/request completes.
func (a *Accumulator) ResponseBody() string {
	if a == nil {
		return ""
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.buf.String()
}

// Truncated reports whether the body was truncated due to maxBytes.
func (a *Accumulator) Truncated() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.truncated
}

// Reset clears the accumulated data for reuse (e.g., between failover retries).
func (a *Accumulator) Reset() {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.buf.Reset()
	a.truncated = false
}

// Enabled reports whether the accumulator is active.
func (a *Accumulator) Enabled() bool {
	if a == nil {
		return false
	}
	return a.enabled
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

type accCtxKey struct{}

// AccumulatorIntoContext stores the Accumulator in the context.
func AccumulatorIntoContext(ctx context.Context, a *Accumulator) context.Context {
	if a == nil || !a.enabled {
		return ctx
	}
	return context.WithValue(ctx, accCtxKey{}, a)
}

// AccumulatorFromContext retrieves the Accumulator from the context. Returns nil if absent.
func AccumulatorFromContext(ctx context.Context) *Accumulator {
	if ctx == nil {
		return nil
	}
	a, _ := ctx.Value(accCtxKey{}).(*Accumulator)
	return a
}
