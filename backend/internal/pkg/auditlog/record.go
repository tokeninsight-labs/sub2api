// Package auditlog provides NDJSON audit logging for LLM gateway requests.
// It records complete request/response payloads to independent files with
// lumberjack-based rotation, async buffered writing, and graceful degradation.
package auditlog

import "time"

// Record represents a single audited request/response pair.
// Serialized as one NDJSON line per record (terminated by '\n').
type Record struct {
	// Identity
	Timestamp time.Time `json:"timestamp"`
	RequestID string    `json:"request_id,omitempty"`
	Endpoint  string    `json:"endpoint"`
	Method    string    `json:"method"`

	// Actors
	UserID    int64  `json:"user_id,omitempty"`
	APIKeyID  int64  `json:"api_key_id,omitempty"`
	GroupID   *int64 `json:"group_id,omitempty"`
	AccountID int64  `json:"account_id"`

	AccountName string `json:"account_name,omitempty"`
	Platform    string `json:"platform,omitempty"`
	Model       string `json:"model,omitempty"`

	// Request
	RequestBody string `json:"request_body"`

	// Response
	ResponseBody   string `json:"response_body"`
	ResponseStatus int    `json:"response_status,omitempty"`

	// Timing & Usage
	Stream       bool   `json:"stream"`
	Transport    string `json:"transport,omitempty"` // "http", "sse", "websocket"
	DurationMs   int64  `json:"duration_ms"`
	FirstTokenMs *int   `json:"first_token_ms,omitempty"`
	InputTokens  int    `json:"input_tokens,omitempty"`
	OutputTokens int    `json:"output_tokens,omitempty"`

	// Client context
	UserAgent string `json:"user_agent,omitempty"`
	ClientIP  string `json:"client_ip,omitempty"`

	// Outcome
	ClientDisconnect bool   `json:"client_disconnect,omitempty"`
	Error            string `json:"error,omitempty"`
	Truncated        bool   `json:"truncated,omitempty"`
}
