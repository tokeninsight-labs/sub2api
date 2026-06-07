package auditlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultAuditFilename = "audit.ndjson"
)

// Config holds the configuration for the audit logger.
type Config struct {
	Directory    string
	MaxSizeMB    int
	MaxBackups   int
	MaxAgeDays   int
	Compress     bool
	BufferSize   int
	Workers      int
	MaxBodyBytes int
}

// Logger is the main entry point for audit logging. It is safe for concurrent use.
// Records are submitted via a non-blocking buffered channel and written asynchronously
// by worker goroutines. If the channel is full, records are dropped gracefully.
type Logger struct {
	cfg    Config
	ch     chan *Record
	writer *lumberjack.Logger
	stopCh chan struct{}
	wg     sync.WaitGroup

	// Metrics
	submitted   atomic.Int64
	dropped     atomic.Int64
	written     atomic.Int64
	writeErrors atomic.Int64
}

// New creates a new audit Logger. Returns an error if the log directory cannot be created.
// The logger starts worker goroutines immediately.
func New(cfg Config) (*Logger, error) {
	dir := resolveAuditDir(cfg.Directory)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("audit log: create directory %s: %w", dir, err)
	}

	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 4096
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}

	lj := &lumberjack.Logger{
		Filename:   filepath.Join(dir, defaultAuditFilename),
		MaxSize:    cfg.MaxSizeMB,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxAgeDays,
		Compress:   cfg.Compress,
		LocalTime:  true,
	}

	l := &Logger{
		cfg:    cfg,
		ch:     make(chan *Record, cfg.BufferSize),
		writer: lj,
		stopCh: make(chan struct{}),
	}

	// Start worker goroutines
	for i := 0; i < cfg.Workers; i++ {
		l.wg.Add(1)
		go l.worker()
	}

	return l, nil
}

// Submit enqueues a record for async writing. Non-blocking: if the buffer is full,
// the record is dropped and the dropped counter is incremented. This guarantees
// audit logging never blocks request processing.
func (l *Logger) Submit(r *Record) {
	if l == nil || r == nil {
		return
	}
	l.submitted.Add(1)

	select {
	case l.ch <- r:
	default:
		l.dropped.Add(1)
	}
}

// Stop drains the channel and closes the underlying file. It blocks until all
// workers finish or the logger is already stopped.
func (l *Logger) Stop() {
	if l == nil {
		return
	}
	select {
	case <-l.stopCh:
		return // already stopped
	default:
	}
	close(l.stopCh)
	close(l.ch) // signal workers to drain and exit
	l.wg.Wait()
	_ = l.writer.Close()
}

// Stats returns the current counters: submitted, dropped, written, writeErrors.
func (l *Logger) Stats() (submitted, dropped, written, writeErrors int64) {
	if l == nil {
		return 0, 0, 0, 0
	}
	return l.submitted.Load(), l.dropped.Load(), l.written.Load(), l.writeErrors.Load()
}

// MaxBodyBytes returns the configured per-body byte limit (0 = unlimited).
func (l *Logger) MaxBodyBytes() int {
	if l == nil {
		return 0
	}
	return l.cfg.MaxBodyBytes
}

func (l *Logger) worker() {
	defer l.wg.Done()
	for r := range l.ch {
		data, err := json.Marshal(r)
		if err != nil {
			l.writeErrors.Add(1)
			logger.L().Warn("audit_log: marshal failed",
				zap.Error(err),
				zap.String("request_id", r.RequestID),
			)
			continue
		}
		data = append(data, '\n')
		if _, err := l.writer.Write(data); err != nil {
			l.writeErrors.Add(1)
			logger.L().Warn("audit_log: write failed",
				zap.Error(err),
				zap.String("request_id", r.RequestID),
			)
			continue
		}
		l.written.Add(1)
	}
}

func resolveAuditDir(explicit string) string {
	explicit = strings.TrimSpace(explicit)
	if explicit != "" {
		return explicit
	}
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir != "" {
		return filepath.Join(dataDir, "audit")
	}
	// Docker default
	if info, err := os.Stat("/app/data"); err == nil && info.IsDir() {
		return "/app/data/audit"
	}
	return "audit"
}

// ---------------------------------------------------------------------------
// Context helpers
// ---------------------------------------------------------------------------

type loggerCtxKey struct{}

// IntoContext stores the audit Logger in the context.
func IntoContext(ctx context.Context, l *Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerCtxKey{}, l)
}

// FromContext retrieves the audit Logger from the context. Returns nil if absent.
func FromContext(ctx context.Context) *Logger {
	if ctx == nil {
		return nil
	}
	l, _ := ctx.Value(loggerCtxKey{}).(*Logger)
	return l
}

// ---------------------------------------------------------------------------
// Convenience: timestamp helpers used by handler layer
// ---------------------------------------------------------------------------

// Now returns the current time (for test injection).
func Now() time.Time {
	return time.Now()
}

// SubmitRecordParams holds all fields needed to build and submit an audit Record.
// Designed to only use primitive/standard-library types so the handler layer can
// call it without circular imports.
type SubmitRecordParams struct {
	StartTime   time.Time
	RequestID   string
	Endpoint    string
	Method      string
	UserID      int64
	APIKeyID    int64
	GroupID     *int64
	AccountID   int64
	AccountName string
	Platform    string
	Model       string
	RequestBody string
	Acc         *Accumulator // may be nil
	Stream      bool
	Transport   string // "http", "sse", "websocket"
	Duration    time.Duration
	FirstTokenMs *int
	InputTokens  int
	OutputTokens int
	UserAgent    string
	ClientIP     string
	ClientDisconnect bool
	Error        string
}

// SubmitRecord builds a Record from the given parameters and enqueues it.
// Safe to call with a nil Logger (no-op).
func (l *Logger) SubmitRecord(p SubmitRecordParams) {
	if l == nil {
		return
	}
	respBody := ""
	truncated := false
	if p.Acc != nil {
		respBody = p.Acc.ResponseBody()
		truncated = p.Acc.Truncated()
	}
	r := &Record{
		Timestamp:        time.Now(),
		RequestID:        p.RequestID,
		Endpoint:         p.Endpoint,
		Method:           p.Method,
		UserID:           p.UserID,
		APIKeyID:         p.APIKeyID,
		GroupID:          p.GroupID,
		AccountID:        p.AccountID,
		AccountName:      p.AccountName,
		Platform:         p.Platform,
		Model:            p.Model,
		RequestBody:      p.RequestBody,
		ResponseBody:     respBody,
		Stream:           p.Stream,
		Transport:        p.Transport,
		DurationMs:       p.Duration.Milliseconds(),
		FirstTokenMs:     p.FirstTokenMs,
		InputTokens:      p.InputTokens,
		OutputTokens:     p.OutputTokens,
		UserAgent:        p.UserAgent,
		ClientIP:         p.ClientIP,
		ClientDisconnect: p.ClientDisconnect,
		Error:            p.Error,
		Truncated:        truncated,
	}
	l.Submit(r)
}
