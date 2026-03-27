package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
)

// ---------- Config ----------

// EndpointConfig represents an OpenAI-compatible API endpoint
type EndpointConfig struct {
	ID           string            `yaml:"id" json:"id"`                       // Unique identifier for this endpoint
	Name         string            `yaml:"name" json:"name"`                   // Display name
	BaseURL      string            `yaml:"base_url" json:"base_url"`           // Base URL (without /v1)
	APIKey       string            `yaml:"api_key" json:"api_key"`             // API key for authentication
	Timeout      int               `yaml:"timeout" json:"timeout"`             // Request timeout in seconds (0 = use global default)
	Headers      map[string]string `yaml:"headers" json:"headers"`             // Extra headers to send with requests
	ExtraParams  map[string]any    `yaml:"extra_params" json:"extra_params"`   // Extra parameters to include in requests
	Enabled      bool              `yaml:"enabled" json:"enabled"`             // Whether this endpoint is active
	Priority     int               `yaml:"priority" json:"priority"`           // Priority for fallback (lower = higher priority)
}

// ModelConfig represents a model configuration
type ModelConfig struct {
	Name                  string         `yaml:"name" json:"name"`                                     // External model name (exposed to AndyAPI)
	UpstreamID            string         `yaml:"upstream_id" json:"upstream_id"`                       // Internal model ID used when calling the endpoint
	EndpointID            string         `yaml:"endpoint_id" json:"endpoint_id"`                       // Which endpoint to use for this model
	MaxCompletionTokens   int            `yaml:"max_completion_tokens" json:"max_completion_tokens"`
	ConcurrentConnections int            `yaml:"concurrent_connections" json:"concurrent_connections"`
	SupportsEmbedding     bool           `yaml:"supports_embedding" json:"supports_embedding"`
	SupportsVision        bool           `yaml:"supports_vision" json:"supports_vision"`
	Fallback              bool           `yaml:"fallback" json:"fallback"`
	Enabled               bool           `yaml:"enabled" json:"enabled"`
	Timeout               int            `yaml:"timeout" json:"timeout"`             // Per-model timeout override (0 = use endpoint default)
	Headers               map[string]string `yaml:"headers" json:"headers"`          // Extra headers for this model
	ExtraParams           map[string]any `yaml:"extra_params" json:"extra_params"`   // Extra parameters for this model
}

// GetUpstreamID returns the model ID to use when calling the upstream API
func (m *ModelConfig) GetUpstreamID() string {
	if m.UpstreamID != "" {
		return m.UpstreamID
	}
	return m.Name
}

// ModelStats tracks statistics for a model
type ModelStats struct {
	TotalRequests       int64     `json:"total_requests"`
	SuccessfulRequests  int64     `json:"successful_requests"`
	FailedRequests      int64     `json:"failed_requests"`
	TotalTokensIn       int64     `json:"total_tokens_in"`
	TotalTokensOut      int64     `json:"total_tokens_out"`
	TotalLatencyMs      int64     `json:"total_latency_ms"`
	AvgLatencyMs        float64   `json:"avg_latency_ms"`
	AvgTokensPerSecond  float64   `json:"avg_tokens_per_second"`
	LastRequestTime     time.Time `json:"last_request_time"`
	LastErrorTime       time.Time `json:"last_error_time,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
}

// StatsTracker manages statistics for all models
type StatsTracker struct {
	mu    sync.RWMutex
	stats map[string]*ModelStats // keyed by model name
}

func NewStatsTracker() *StatsTracker {
	return &StatsTracker{
		stats: make(map[string]*ModelStats),
	}
}

func (st *StatsTracker) RecordRequest(modelName string, success bool, latencyMs int64, tokensIn, tokensOut int) {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	s, ok := st.stats[modelName]
	if !ok {
		s = &ModelStats{}
		st.stats[modelName] = s
	}
	
	atomic.AddInt64(&s.TotalRequests, 1)
	if success {
		atomic.AddInt64(&s.SuccessfulRequests, 1)
		atomic.AddInt64(&s.TotalTokensIn, int64(tokensIn))
		atomic.AddInt64(&s.TotalTokensOut, int64(tokensOut))
		atomic.AddInt64(&s.TotalLatencyMs, latencyMs)
		s.LastRequestTime = time.Now()
		
		// Calculate averages
		if s.SuccessfulRequests > 0 {
			s.AvgLatencyMs = float64(s.TotalLatencyMs) / float64(s.SuccessfulRequests)
			if s.TotalLatencyMs > 0 {
				s.AvgTokensPerSecond = float64(s.TotalTokensOut) / (float64(s.TotalLatencyMs) / 1000.0)
			}
		}
	} else {
		atomic.AddInt64(&s.FailedRequests, 1)
		s.LastErrorTime = time.Now()
	}
}

func (st *StatsTracker) RecordError(modelName string, err string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	
	s, ok := st.stats[modelName]
	if !ok {
		s = &ModelStats{}
		st.stats[modelName] = s
	}
	s.LastError = err
	s.LastErrorTime = time.Now()
}

func (st *StatsTracker) GetStats(modelName string) *ModelStats {
	st.mu.RLock()
	defer st.mu.RUnlock()
	if s, ok := st.stats[modelName]; ok {
		// Return a copy
		copy := *s
		return &copy
	}
	return &ModelStats{}
}

func (st *StatsTracker) GetAllStats() map[string]*ModelStats {
	st.mu.RLock()
	defer st.mu.RUnlock()
	result := make(map[string]*ModelStats)
	for k, v := range st.stats {
		copy := *v
		result[k] = &copy
	}
	return result
}

func (st *StatsTracker) Reset(modelName string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	delete(st.stats, modelName)
}

func (st *StatsTracker) ResetAll() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.stats = make(map[string]*ModelStats)
}

type Config struct {
	// Base URL of Andy API. Examples:
	//  http://localhost:8080  -> ws://localhost:8080/ws
	//  https://api.example.com -> wss://api.example.com/ws
	//  ws://host:port/ws (kept, /ws appended if missing)
	//  wss://host:port/ws
	AndyAPIURL string `yaml:"andy_api_url" json:"andy_api_url"`
	// Client token for user authentication (obtained from AndyAPI admin panel)
	ClientToken       string `yaml:"client_token" json:"client_token"`
	Provider          string `yaml:"provider" json:"provider"`
	HeartbeatInterval int    `yaml:"heartbeat_interval" json:"heartbeat_interval"`
	ReconnectMaxBack  int    `yaml:"reconnect_max_backoff" json:"reconnect_max_backoff"`
	
	// Global default timeout in seconds (default: 120)
	DefaultTimeout int `yaml:"default_timeout" json:"default_timeout"`
	
	// Hot reload configuration changes (default: true)
	HotReload bool `yaml:"hot_reload" json:"hot_reload"`
	
	// Multiple OpenAI-compatible endpoints
	Endpoints []EndpointConfig `yaml:"endpoints" json:"endpoints"`
	
	// Legacy single endpoint fields (for backward compatibility)
	LocalAPIURL  string        `yaml:"local_api_url" json:"local_api_url"`
	LocalAPIKey  string        `yaml:"local_api_key" json:"local_api_key"`
	
	Models       []ModelConfig `yaml:"models" json:"models"`
	LastClientID string        `yaml:"last_client_id" json:"last_client_id"`
}

// GetEndpoint returns the endpoint configuration for a given ID
func (c *Config) GetEndpoint(id string) *EndpointConfig {
	for i := range c.Endpoints {
		if c.Endpoints[i].ID == id {
			return &c.Endpoints[i]
		}
	}
	return nil
}

// GetDefaultEndpoint returns the first enabled endpoint or creates one from legacy config
func (c *Config) GetDefaultEndpoint() *EndpointConfig {
	// First, try to find an enabled endpoint
	for i := range c.Endpoints {
		if c.Endpoints[i].Enabled {
			return &c.Endpoints[i]
		}
	}
	// Fall back to legacy config if available
	if c.LocalAPIURL != "" {
		return &EndpointConfig{
			ID:      "default",
			Name:    "Default",
			BaseURL: c.LocalAPIURL,
			APIKey:  c.LocalAPIKey,
			Enabled: true,
		}
	}
	return nil
}

// GetEndpointForModel returns the endpoint to use for a given model
func (c *Config) GetEndpointForModel(model *ModelConfig) *EndpointConfig {
	if model.EndpointID != "" {
		if ep := c.GetEndpoint(model.EndpointID); ep != nil {
			return ep
		}
	}
	return c.GetDefaultEndpoint()
}

// GetTimeoutForModel returns the timeout to use for a model request
func (c *Config) GetTimeoutForModel(model *ModelConfig) time.Duration {
	// Model-level timeout takes precedence
	if model.Timeout > 0 {
		return time.Duration(model.Timeout) * time.Second
	}
	// Then endpoint-level timeout
	if ep := c.GetEndpointForModel(model); ep != nil && ep.Timeout > 0 {
		return time.Duration(ep.Timeout) * time.Second
	}
	// Then global default
	if c.DefaultTimeout > 0 {
		return time.Duration(c.DefaultTimeout) * time.Second
	}
	// Fallback to 120 seconds
	return 120 * time.Second
}

// GetModelByName returns a model configuration by its external name
func (c *Config) GetModelByName(name string) *ModelConfig {
	for i := range c.Models {
		if c.Models[i].Name == name {
			return &c.Models[i]
		}
	}
	return nil
}

func (c *Config) WSURL() string {
	base := strings.TrimSpace(c.AndyAPIURL)
	if base == "" {
		return "ws://localhost:8080/ws"
	}
	// trim trailing slashes
	for strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}
	if strings.HasPrefix(base, "ws://") || strings.HasPrefix(base, "wss://") {
		if strings.HasSuffix(base, "/ws") {
			return base
		}
		return base + "/ws"
	}
	if strings.HasPrefix(base, "http://") {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	} else if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else {
		base = "ws://" + base
	}
	return base + "/ws"
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return nil, err
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 30
	}
	if c.ReconnectMaxBack <= 0 {
		c.ReconnectMaxBack = 30
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 120
	}
	// Default hot reload to true
	if !c.HotReload {
		c.HotReload = true
	}
	// Migrate legacy config to endpoints if needed
	if len(c.Endpoints) == 0 && c.LocalAPIURL != "" {
		c.Endpoints = []EndpointConfig{{
			ID:      "default",
			Name:    "Default",
			BaseURL: c.LocalAPIURL,
			APIKey:  c.LocalAPIKey,
			Enabled: true,
		}}
	}
	return c, nil
}

// ---------- WS Protocol Structures (mirrors server) ----------

type WSMessage struct {
	Type      string      `json:"type"`
	ClientID  string      `json:"client_id,omitempty"`
	RequestID string      `json:"request_id,omitempty"`
	Data      interface{} `json:"data"`
	Timestamp time.Time   `json:"timestamp"`
}

type ProvidedModel struct {
	ClientID              string  `json:"client_id"`
	Provider              string  `json:"provider"`
	Name                  string  `json:"name"`
	UpstreamID            string  `json:"upstream_id,omitempty"`
	MaxCompletionTokens   int     `json:"max_completion_tokens"`
	ConcurrentConnections int     `json:"concurrent_connections"`
	AvgTokensPerSecond    float64 `json:"avg_tokens_per_second"`
	Latency               float64 `json:"latency"`
	SuccessfulResponses   int     `json:"successful_responses"`
	FailedResponses       int     `json:"failed_responses"`
	SupportsEmbedding     bool    `json:"supports_embedding"`
	SupportsVision        bool    `json:"supports_vision"`
	Fallback              bool    `json:"fallback"`
	IsAvailable           bool    `json:"is_available"`
	IsServerModel         bool    `json:"is_server_model"`
	Tier                  string  `json:"tier,omitempty"`
}

type ClientRegistration struct {
	ID          string          `json:"id,omitempty"`
	ClientToken string          `json:"client_token,omitempty"`
	Models      []ProvidedModel `json:"models"`
}

// ChatCompletionMessage mirrors OpenAI message format
type ChatCompletionMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type LocalClientRequest struct {
	ID                  int                     `json:"id"`
	Prompt              string                  `json:"prompt"`
	Messages            []ChatCompletionMessage `json:"messages,omitempty"`
	ImageBase64         string                  `json:"image_base64"`
	Model               string                  `json:"model"`
	MaxCompletionTokens int                     `json:"max_completion_tokens"`
	Task                string                  `json:"task,omitempty"` // "chat", "embedding"
}

type LocalClientResponse struct {
	ID       int    `json:"id"`
	Response string `json:"response"`
	Status   string `json:"status"`
	Error    int    `json:"error"`
}

// ---------- Runtime Client State ----------

type ProviderClient struct {
	cfg           *Config
	conn          *websocket.Conn
	mu            sync.RWMutex
	writeMu       sync.Mutex
	clientID      string
	connected     bool
	registered    bool // true after server sends welcome with client_id
	everConnected bool
	closing       chan struct{}
	closed        bool
	httpSrv       *http.Server
	configPath    string
	initialSetup  bool
	connectCtx    context.Context
	connectCancel context.CancelFunc
	stats         *StatsTracker
	configWatcher *fsnotify.Watcher
	lastSelfSave  time.Time // tracks our own config saves to avoid self-triggered reloads
}

func NewProviderClient(cfg *Config, configPath string, initial bool) *ProviderClient {
	return &ProviderClient{
		cfg:        cfg,
		closing:    make(chan struct{}),
		configPath: configPath,
		initialSetup: initial,
		stats:      NewStatsTracker(),
	}
}

// startConfigWatcher watches the config file for changes and reloads it
func (pc *ProviderClient) startConfigWatcher() {
	if !pc.cfg.HotReload {
		return
	}
	
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("Failed to create config watcher: %v", err)
		return
	}
	pc.configWatcher = watcher
	
	go func() {
		defer watcher.Close()
		for {
			select {
			case <-pc.closing:
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&(fsnotify.Write|fsnotify.Create) != 0 {
					log.Printf("Config file changed, reloading...")
					time.Sleep(100 * time.Millisecond) // Debounce
					if err := pc.reloadConfig(); err != nil {
						log.Printf("Failed to reload config: %v", err)
					} else {
						log.Printf("Config reloaded successfully")
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Config watcher error: %v", err)
			}
		}
	}()
	
	// Watch the config file's directory to handle file replacements
	configDir := filepath.Dir(pc.configPath)
	if err := watcher.Add(configDir); err != nil {
		log.Printf("Failed to watch config directory: %v", err)
	}
	if err := watcher.Add(pc.configPath); err != nil {
		log.Printf("Failed to watch config file: %v", err)
	}
}

// reloadConfig reloads the configuration from disk
func (pc *ProviderClient) reloadConfig() error {
	newCfg, err := loadConfig(pc.configPath)
	if err != nil {
		return err
	}
	
	pc.mu.Lock()
	oldURL := pc.cfg.WSURL()
	pc.cfg = newCfg
	newURL := pc.cfg.WSURL()
	pc.mu.Unlock()
	
	// If the AndyAPI URL changed and we're connected, we need to reconnect
	if oldURL != newURL && pc.connected {
		log.Printf("AndyAPI URL changed, reconnecting...")
		pc.StopConnect()
		pc.StartConnect()
	} else if pc.connected && pc.registered {
		// Skip broadcasting if this reload was triggered by our own saveConfig
		// (prevents update_models from racing with register on the server)
		if time.Since(pc.lastSelfSave) < 2*time.Second {
			log.Printf("Config reload: skipping broadcast (self-triggered save)")
		} else {
			log.Printf("Config changed externally, broadcasting model update")
			pc.broadcastModelUpdate()
		}
	}
	
	return nil
}

// broadcastModelUpdate sends the current model list to the server
func (pc *ProviderClient) broadcastModelUpdate() {
	pc.mu.RLock()
	models := pc.buildProvidedModels()
	pc.mu.RUnlock()
	pc.writeJSON(WSMessage{Type: "update_models", Data: models, Timestamp: time.Now()})
}

// buildProvidedModels creates the ProvidedModel list from config
func (pc *ProviderClient) buildProvidedModels() []ProvidedModel {
	models := make([]ProvidedModel, 0, len(pc.cfg.Models))
	// Use current clientID, falling back to saved LastClientID (matches pre-refactor behavior)
	cid := pc.clientID
	if cid == "" {
		cid = strings.TrimSpace(pc.cfg.LastClientID)
	}
	for _, m := range pc.cfg.Models {
		if !m.Enabled {
			continue
		}
		stats := pc.stats.GetStats(m.Name)
		models = append(models, ProvidedModel{
			ClientID:              cid,
			Provider:              pc.cfg.Provider,
			Name:                  m.Name,
			UpstreamID:            "", // Force server to send requests using the internal Name, not the UpstreamID
			MaxCompletionTokens:   m.MaxCompletionTokens,
			ConcurrentConnections: m.ConcurrentConnections,
			AvgTokensPerSecond:    stats.AvgTokensPerSecond,
			Latency:               stats.AvgLatencyMs,
			SuccessfulResponses:   int(stats.SuccessfulRequests),
			FailedResponses:       int(stats.FailedRequests),
			SupportsEmbedding:     m.SupportsEmbedding,
			SupportsVision:        m.SupportsVision,
			Fallback:              m.Fallback,
			IsAvailable:           m.Enabled,
			IsServerModel:         false,
		})
	}
	return models
}

// connect establishes WS connection with exponential backoff
func (pc *ProviderClient) connect(ctx context.Context) {
	backoff := 1
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		wsURL := pc.cfg.WSURL()

		// Append token as query parameter (backend checks both query param and header)
		if token := strings.TrimSpace(pc.cfg.ClientToken); token != "" {
			if strings.Contains(wsURL, "?") {
				wsURL = wsURL + "&token=" + token
			} else {
				wsURL = wsURL + "?token=" + token
			}
		}

		log.Printf("Connecting to Andy API WS: %s", wsURL)

		// Also send token in header as backup
		headers := http.Header{}
		if token := strings.TrimSpace(pc.cfg.ClientToken); token != "" {
			headers.Set("X-Client-Token", token)
		}

		c, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
		if err != nil {
			if resp != nil {
				log.Printf("Dial failed: %v (HTTP %d)", err, resp.StatusCode)
			} else {
				log.Printf("Dial failed: %v", err)
			}
			backoff = min(backoff*2, pc.cfg.ReconnectMaxBack)
			time.Sleep(time.Duration(backoff) * time.Second)
			continue
		}
		pc.mu.Lock()
		pc.conn = c
		pc.connected = true
		pc.mu.Unlock()
		log.Printf("WebSocket connected")
		pc.handleConnection(ctx)
		backoff = 1
		// After connection ends mark disconnected and retry on next loop
		pc.mu.Lock()
		pc.connected = false
		pc.registered = false
		pc.mu.Unlock()
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handleConnection manages registration, read & heartbeat loops until disconnect
func (pc *ProviderClient) handleConnection(ctx context.Context) {
	pc.mu.RLock()
	models := pc.buildProvidedModels()
	lastID := strings.TrimSpace(pc.cfg.LastClientID)
	clientToken := strings.TrimSpace(pc.cfg.ClientToken)
	pc.mu.RUnlock()
	
	reg := WSMessage{Type: "register", ClientID: lastID, Data: ClientRegistration{ID: lastID, ClientToken: clientToken, Models: models}, Timestamp: time.Now()}
	pc.writeJSON(reg)
	// Setup pong handler to extend deadlines
	pc.conn.SetPongHandler(func(appData string) error {
		pc.conn.SetReadDeadline(time.Now().Add(time.Duration(pc.cfg.HeartbeatInterval*2) * time.Second))
		return nil
	})
	go pc.heartbeatLoop()
	pc.readLoop(ctx)
}

func (pc *ProviderClient) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(pc.cfg.HeartbeatInterval) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-pc.closing:
			return
		case <-ticker.C:
			// Send app-level heartbeat message
			pc.writeJSON(WSMessage{Type: "heartbeat", Timestamp: time.Now()})
			// Also try a websocket-level ping to keep NATs happy
			pc.mu.RLock()
			c := pc.conn
			pc.mu.RUnlock()
			if c != nil {
				pc.writeMu.Lock()
				_ = c.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(5*time.Second))
				pc.writeMu.Unlock()
			}
		}
	}
}

func (pc *ProviderClient) readLoop(ctx context.Context) {
	for {
		pc.conn.SetReadDeadline(time.Now().Add(time.Duration(pc.cfg.HeartbeatInterval*2) * time.Second))
		_, data, err := pc.conn.ReadMessage()
		if err != nil {
			log.Printf("Read error: %v", err)
			pc.conn.Close()
			return
		}
		var msg WSMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		switch msg.Type {
		case "welcome":
			if d, ok := msg.Data.(map[string]interface{}); ok {
				if id, ok2 := d["client_id"].(string); ok2 {
					pc.mu.Lock()
					pc.clientID = id
					pc.registered = true
					pc.everConnected = true
					pc.cfg.LastClientID = id
					_ = pc.saveConfig("")
					pc.mu.Unlock()
					log.Printf("Assigned client_id=%s", id)
				}
			}
		case "request":
			// Handle in background to avoid blocking the read loop (prevents ping/pong timeouts)
			go pc.handleRequest(msg)
		}
	}
}

func (pc *ProviderClient) handleRequest(msg WSMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in handleRequest: %v", r)
		}
	}()
	var req LocalClientRequest
	b, _ := json.Marshal(msg.Data)
	_ = json.Unmarshal(b, &req)
	
	startTime := time.Now()
	
	// Forward to local OpenAI-compatible API using the requested model
	respText, tokensIn, tokensOut, err := pc.callLocalCompletion(req)
	
	latencyMs := time.Since(startTime).Milliseconds()
	
	if err != nil {
		log.Printf("local completion error: %v", err)
		pc.stats.RecordRequest(req.Model, false, latencyMs, 0, 0)
		pc.stats.RecordError(req.Model, err.Error())
		resp := LocalClientResponse{ID: req.ID, Response: "", Status: "error", Error: 1}
		pc.writeJSON(WSMessage{Type: "response", RequestID: msg.RequestID, ClientID: pc.clientID, Data: resp, Timestamp: time.Now()})
		return
	}
	
	pc.stats.RecordRequest(req.Model, true, latencyMs, tokensIn, tokensOut)
	resp := LocalClientResponse{ID: req.ID, Response: respText, Status: "ok", Error: 0}
	pc.writeJSON(WSMessage{Type: "response", RequestID: msg.RequestID, ClientID: pc.clientID, Data: resp, Timestamp: time.Now()})
}

// callLocalCompletion sends a chat completion request to the configured local OpenAI-compatible API.
// Returns: response text, tokens in, tokens out, error
func (pc *ProviderClient) callLocalCompletion(req LocalClientRequest) (string, int, int, error) {
	pc.mu.RLock()
	cfg := pc.cfg
	pc.mu.RUnlock()
	
	// Find the model configuration
	model := cfg.GetModelByName(req.Model)
	if model == nil {
		// Create a temporary model config for unknown models
		model = &ModelConfig{Name: req.Model}
	}
	
	// Get the endpoint for this model
	endpoint := cfg.GetEndpointForModel(model)
	if endpoint == nil {
		return "", 0, 0, fmt.Errorf("no endpoint configured for model %s", req.Model)
	}
	
	base := strings.TrimSpace(endpoint.BaseURL)
	if base == "" {
		return "", 0, 0, fmt.Errorf("endpoint base_url not configured")
	}
	for strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}

	// Handle embedding task
	if req.Task == "embedding" {
		result, err := pc.callLocalEmbedding(endpoint, model, req)
		return result, 0, 0, err
	}

	url := base + "/chat/completions"
	// Build messages: prefer Messages array, fallback to Prompt
	var messages []map[string]string
	if len(req.Messages) > 0 {
		for _, m := range req.Messages {
			messages = append(messages, map[string]string{"role": m.Role, "content": m.Content})
		}
	} else if req.Prompt != "" {
		messages = []map[string]string{{"role": "user", "content": req.Prompt}}
	} else {
		return "", 0, 0, fmt.Errorf("no messages or prompt provided")
	}

	// Use upstream model ID if configured
	modelID := model.GetUpstreamID()

	payload := map[string]interface{}{
		"model":    modelID,
		"messages": messages,
	}
	
	// Add max_tokens if > 0
	if req.MaxCompletionTokens > 0 {
		payload["max_completion_tokens"] = req.MaxCompletionTokens
	}
	
	// Merge extra parameters: endpoint params first, then model params (model takes precedence)
	for k, v := range endpoint.ExtraParams {
		payload[k] = v
	}
	for k, v := range model.ExtraParams {
		payload[k] = v
	}
	
	body, _ := json.Marshal(payload)
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	
	// Set API key if configured
	if endpoint.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+endpoint.APIKey)
	}
	
	// Add extra headers: endpoint headers first, then model headers (model takes precedence)
	for k, v := range endpoint.Headers {
		httpReq.Header.Set(k, v)
	}
	for k, v := range model.Headers {
		httpReq.Header.Set(k, v)
	}
	
	timeout := cfg.GetTimeoutForModel(model)
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var slurp struct {
			Error interface{} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&slurp)
		return "", 0, 0, fmt.Errorf("local api status %s: %v", resp.Status, slurp.Error)
	}
	var out struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", 0, 0, err
	}
	if len(out.Choices) == 0 {
		return "", 0, 0, fmt.Errorf("no choices")
	}
	return out.Choices[0].Message.Content, out.Usage.PromptTokens, out.Usage.CompletionTokens, nil
}

// callLocalEmbedding sends an embedding request to the configured local OpenAI-compatible API.
func (pc *ProviderClient) callLocalEmbedding(endpoint *EndpointConfig, model *ModelConfig, req LocalClientRequest) (string, error) {
	base := strings.TrimSpace(endpoint.BaseURL)
	for strings.HasSuffix(base, "/") {
		base = strings.TrimSuffix(base, "/")
	}
	url := base + "/embeddings"
	
	// Build input from prompt or first message
	input := req.Prompt
	if input == "" && len(req.Messages) > 0 {
		input = req.Messages[len(req.Messages)-1].Content
	}
	
	// Use upstream model ID if configured
	modelID := model.GetUpstreamID()
	
	payload := map[string]interface{}{
		"model": modelID,
		"input": input,
	}
	
	// Merge extra parameters
	for k, v := range endpoint.ExtraParams {
		payload[k] = v
	}
	for k, v := range model.ExtraParams {
		payload[k] = v
	}
	
	body, _ := json.Marshal(payload)
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	
	if endpoint.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+endpoint.APIKey)
	}
	
	// Add extra headers
	for k, v := range endpoint.Headers {
		httpReq.Header.Set(k, v)
	}
	for k, v := range model.Headers {
		httpReq.Header.Set(k, v)
	}
	
	pc.mu.RLock()
	timeout := pc.cfg.GetTimeoutForModel(model)
	pc.mu.RUnlock()
	
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var slurp struct {
			Error interface{} `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&slurp)
		return "", fmt.Errorf("local api status %s: %v", resp.Status, slurp.Error)
	}
	// Return raw JSON response for embeddings (server will parse it)
	var rawResp json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&rawResp); err != nil {
		return "", err
	}
	return string(rawResp), nil
}

func (pc *ProviderClient) writeJSON(v interface{}) {
	pc.mu.RLock()
	c := pc.conn
	pc.mu.RUnlock()
	if c == nil {
		return
	}
	// Serialize all writes; gorilla/websocket requires application-level write locking
	pc.writeMu.Lock()
	defer pc.writeMu.Unlock()
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := c.WriteJSON(v); err != nil {
		log.Printf("Write error: %v", err)
	}
}

// saveConfig writes current config to disk if persistence enabled
func (pc *ProviderClient) saveConfig(path string) error {
	if path == "" {
		path = pc.configPath
		if path == "" {
			path = "config.yaml"
		}
	}
	b, err := yaml.Marshal(pc.cfg)
	if err != nil {
		return err
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		_ = os.MkdirAll(dir, 0755)
	}
	pc.lastSelfSave = time.Now()
	return os.WriteFile(path, b, 0644)
}

// ---------- Management HTTP API ----------
func (pc *ProviderClient) startHTTP(addr string) {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/", func(c *gin.Context) { c.File("ui/index.html") })
	r.GET("/ui", func(c *gin.Context) { c.File("ui/index.html") })
	r.GET("/api/state", func(c *gin.Context) { c.JSON(200, gin.H{"initial_setup": pc.initialSetup}) })
	r.GET("/config", func(c *gin.Context) { c.JSON(200, pc.cfg) })
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "client_id": pc.clientID, "ws_url": pc.cfg.WSURL()})
	})
	r.GET("/api/status", func(c *gin.Context) {
		pc.mu.RLock()
		clientID := pc.clientID
		if clientID == "" {
			clientID = pc.cfg.LastClientID
		}
		status := gin.H{
			"connected":      pc.connected,
			"ever_connected": pc.everConnected || (clientID != ""),
			"client_id":      clientID,
			"ws_url":         pc.cfg.WSURL(),
		}
		pc.mu.RUnlock()
		c.JSON(200, status)
	})
	
	// Statistics endpoints
	r.GET("/api/stats", func(c *gin.Context) {
		c.JSON(200, gin.H{"stats": pc.stats.GetAllStats()})
	})
	r.GET("/api/stats/:model", func(c *gin.Context) {
		modelName := c.Param("model")
		c.JSON(200, pc.stats.GetStats(modelName))
	})
	r.POST("/api/stats/reset", func(c *gin.Context) {
		var body struct {
			Model string `json:"model"`
		}
		if err := c.ShouldBindJSON(&body); err == nil && body.Model != "" {
			pc.stats.Reset(body.Model)
		} else {
			pc.stats.ResetAll()
		}
		c.JSON(200, gin.H{"ok": true})
	})
	
	// Endpoint management
	r.GET("/api/endpoints", func(c *gin.Context) {
		pc.mu.RLock()
		endpoints := pc.cfg.Endpoints
		pc.mu.RUnlock()
		c.JSON(200, gin.H{"endpoints": endpoints})
	})
	r.POST("/api/endpoints", func(c *gin.Context) {
		var endpoints []EndpointConfig
		if err := c.ShouldBindJSON(&endpoints); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		pc.mu.Lock()
		pc.cfg.Endpoints = endpoints
		pc.mu.Unlock()
		_ = pc.saveConfig("")
		c.JSON(200, gin.H{"saved": true, "count": len(endpoints)})
	})
	r.POST("/api/endpoints/add", func(c *gin.Context) {
		var endpoint EndpointConfig
		if err := c.ShouldBindJSON(&endpoint); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if endpoint.ID == "" {
			endpoint.ID = fmt.Sprintf("endpoint-%d", time.Now().UnixNano())
		}
		pc.mu.Lock()
		pc.cfg.Endpoints = append(pc.cfg.Endpoints, endpoint)
		pc.mu.Unlock()
		_ = pc.saveConfig("")
		c.JSON(200, gin.H{"saved": true, "endpoint": endpoint})
	})
	r.DELETE("/api/endpoints/:id", func(c *gin.Context) {
		endpointID := c.Param("id")
		pc.mu.Lock()
		newEndpoints := make([]EndpointConfig, 0, len(pc.cfg.Endpoints))
		for _, ep := range pc.cfg.Endpoints {
			if ep.ID != endpointID {
				newEndpoints = append(newEndpoints, ep)
			}
		}
		pc.cfg.Endpoints = newEndpoints
		pc.mu.Unlock()
		_ = pc.saveConfig("")
		c.JSON(200, gin.H{"deleted": true})
	})
	
	r.POST("/api/connect", func(c *gin.Context) {
		pc.StartConnect()
		c.JSON(200, gin.H{"ok": true})
	})
	r.POST("/api/disconnect", func(c *gin.Context) {
		pc.StopConnect()
		c.JSON(200, gin.H{"ok": true})
	})
	r.POST("/api/save-config", func(c *gin.Context) {
		if !pc.initialSetup {
			c.JSON(400, gin.H{"error": "not in initial setup"})
			return
		}
		var newCfg Config
		if err := c.ShouldBindJSON(&newCfg); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if strings.TrimSpace(newCfg.AndyAPIURL) == "" {
			c.JSON(400, gin.H{"error": "andy_api_url required"})
			return
		}
		if strings.TrimSpace(newCfg.Provider) == "" {
			newCfg.Provider = "provider"
		}
		if len(newCfg.Models) == 0 {
			c.JSON(400, gin.H{"error": "at least one model"})
			return
		}
		pc.mu.Lock()
		pc.cfg = &newCfg
		pc.initialSetup = false
		pc.mu.Unlock()
		if err := pc.saveConfig(""); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		pc.broadcastModelUpdate()
		c.JSON(200, gin.H{"saved": true})
	})
	r.POST("/models", func(c *gin.Context) {
		var models []ModelConfig
		if err := c.ShouldBindJSON(&models); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		pc.mu.Lock()
		pc.cfg.Models = models
		pc.mu.Unlock()
		pc.broadcastModelUpdate()
		_ = pc.saveConfig("")
		c.JSON(200, gin.H{"updated": len(models)})
	})

	// Update full config even after initial setup (allows changing base URL, provider, etc.)
	r.POST("/api/update-config", func(c *gin.Context) {
		var newCfg Config
		if err := c.ShouldBindJSON(&newCfg); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if strings.TrimSpace(newCfg.AndyAPIURL) == "" {
			c.JSON(400, gin.H{"error": "andy_api_url required"})
			return
		}
		if strings.TrimSpace(newCfg.Provider) == "" {
			newCfg.Provider = "provider"
		}
		// Merge: if no models provided, keep existing
		if len(newCfg.Models) == 0 {
			newCfg.Models = pc.cfg.Models
		}
		// Merge: if no endpoints provided, keep existing
		if len(newCfg.Endpoints) == 0 {
			newCfg.Endpoints = pc.cfg.Endpoints
		}
		pc.mu.Lock()
		pc.cfg.AndyAPIURL = newCfg.AndyAPIURL
		pc.cfg.ClientToken = newCfg.ClientToken
		pc.cfg.LocalAPIURL = newCfg.LocalAPIURL
		pc.cfg.LocalAPIKey = newCfg.LocalAPIKey
		pc.cfg.Provider = newCfg.Provider
		pc.cfg.HeartbeatInterval = newCfg.HeartbeatInterval
		pc.cfg.ReconnectMaxBack = newCfg.ReconnectMaxBack
		pc.cfg.DefaultTimeout = newCfg.DefaultTimeout
		pc.cfg.HotReload = newCfg.HotReload
		pc.cfg.Models = newCfg.Models
		pc.cfg.Endpoints = newCfg.Endpoints
		pc.mu.Unlock()
		if err := pc.saveConfig(""); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		// Broadcast model updates
		pc.broadcastModelUpdate()
		c.JSON(200, gin.H{"saved": true})
	})
	
	// Reload config from disk
	r.POST("/api/reload-config", func(c *gin.Context) {
		if err := pc.reloadConfig(); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"reloaded": true})
	})

	// Scan models from a local OpenAI-compatible endpoint to avoid browser CORS issues.
	r.POST("/api/scan-models", func(c *gin.Context) {
		var body struct {
			BaseURL    string `json:"base_url"`
			APIKey     string `json:"api_key"`
			EndpointID string `json:"endpoint_id"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		
		base := strings.TrimSpace(body.BaseURL)
		apiKey := strings.TrimSpace(body.APIKey)
		
		// If endpoint_id is provided, use that endpoint's config
		if body.EndpointID != "" {
			if ep := pc.cfg.GetEndpoint(body.EndpointID); ep != nil {
				if base == "" {
					base = ep.BaseURL
				}
				if apiKey == "" {
					apiKey = ep.APIKey
				}
			}
		}
		
		if base == "" {
			c.JSON(400, gin.H{"error": "base_url required"})
			return
		}
		// normalize URL
		for strings.HasSuffix(base, "/") {
			base = strings.TrimSuffix(base, "/")
		}
		url := base + "/models"
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		if apiKey != "" {
			req.Header.Set("Authorization", "Bearer "+apiKey)
		}
		req.Header.Set("Accept", "application/json")
		httpClient := &http.Client{Timeout: 10 * time.Second}
		resp, err := httpClient.Do(req)
		if err != nil {
			c.JSON(502, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			c.JSON(resp.StatusCode, gin.H{"error": "remote returned status " + resp.Status})
			return
		}
		var payload struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		models := make([]ModelConfig, 0, len(payload.Data))
		for _, item := range payload.Data {
			name := strings.TrimSpace(item.ID)
			if name == "" {
				continue
			}
			m := ModelConfig{
				Name:                  name,
				UpstreamID:            name, // Same as name by default
				EndpointID:            body.EndpointID,
				MaxCompletionTokens:   4096,
				ConcurrentConnections: 1,
				Enabled:               false,
			}
			// crude heuristics
			lname := strings.ToLower(name)
			if strings.Contains(lname, "embed") {
				m.SupportsEmbedding = true
			}
			if strings.Contains(lname, "vision") || strings.Contains(lname, "vl") {
				m.SupportsVision = true
			}
			models = append(models, m)
		}
		c.JSON(200, gin.H{"models": models})
	})
	pc.httpSrv = &http.Server{Addr: addr, Handler: r}
	go func() {
		if err := pc.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("mgmt http error: %v", err)
		}
	}()
}

// Manual connect/disconnect controls
func (pc *ProviderClient) StartConnect() {
	pc.mu.Lock()
	if pc.connectCtx != nil {
		pc.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	pc.connectCtx = ctx
	pc.connectCancel = cancel
	pc.mu.Unlock()
	go pc.connect(ctx)
}

func (pc *ProviderClient) StopConnect() {
	pc.mu.Lock()
	if pc.connectCancel != nil {
		pc.connectCancel()
		pc.connectCancel = nil
		pc.connectCtx = nil
	}
	c := pc.conn
	pc.mu.Unlock()
	if c != nil {
		_ = c.Close()
	}
}

// ---------- Entry Point ----------
func main() {
	rand.Seed(time.Now().UnixNano())
	cfgPath := flag.String("config", "config.yaml", "Path to config.yaml")
	example := flag.String("example", "config.example.yaml", "Path to example config (used if config missing)")
	httpAddr := flag.String("http", ":8090", "Management HTTP listen address")
	flag.Parse()
	var cfg *Config
	initial := false
	if _, err := os.Stat(*cfgPath); err != nil {
		// attempt example
		if ex, err2 := loadConfig(*example); err2 == nil {
			cfg = ex
			initial = true
			log.Printf("config not found; starting in initial-setup mode")
		} else {
			// create minimal default
			cfg = &Config{
				AndyAPIURL:        "http://localhost:8080",
				Provider:          "provider",
				HeartbeatInterval: 30,
				ReconnectMaxBack:  30,
				DefaultTimeout:    120,
				HotReload:         true,
			}
			initial = true
			log.Printf("config & example missing; using defaults for initial setup")
		}
	} else {
		c, err := loadConfig(*cfgPath)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
		cfg = c
	}
	client := NewProviderClient(cfg, *cfgPath, initial)
	_, cancel := context.WithCancel(context.Background())
	
	// Start config file watcher for hot reload
	client.startConfigWatcher()
	
	// Do not autoconnect; UI will call /api/connect
	client.startHTTP(*httpAddr)

	// Print startup message with instructions
	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════════════════╗")
	fmt.Println("║             AndyAPI Local Client - Provider Mode                 ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════╣")
	fmt.Println("║  Web UI available at:                                            ║")
	fmt.Printf("║    → http://localhost%s                                        ║\n", *httpAddr)
	fmt.Println("║                                                                  ║")
	fmt.Println("║  Quick Start:                                                    ║")
	fmt.Println("║    1. Open the Web UI in your browser                            ║")
	fmt.Println("║    2. Enter your AndyAPI server URL                              ║")
	fmt.Println("║    3. Enter your Client Token (from AndyAPI admin panel)         ║")
	fmt.Println("║    4. Configure your Local OpenAI API URL (e.g. Ollama)          ║")
	fmt.Println("║    5. Scan or add models, then click 'Save'                      ║")
	fmt.Println("║    6. Click 'Connect to AndyAPI' to start providing models       ║")
	fmt.Println("║                                                                  ║")
	fmt.Println("║  New Features:                                                   ║")
	fmt.Println("║    • Multiple API endpoints with custom headers/params           ║")
	fmt.Println("║    • Statistics tracking (view at /api/stats)                    ║")
	fmt.Println("║    • Hot reload config (edit config.yaml while running)          ║")
	fmt.Println("║    • Configurable timeouts per endpoint/model                    ║")
	fmt.Println("║                                                                  ║")
	fmt.Println("║  Press Ctrl+C to stop the client.                                ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Println("Shutting down provider client...")
	close(client.closing)
	cancel()
	client.StopConnect()
	if client.configWatcher != nil {
		_ = client.configWatcher.Close()
	}
	if client.httpSrv != nil {
		_ = client.httpSrv.Shutdown(context.Background())
	}
}
