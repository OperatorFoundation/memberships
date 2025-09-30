package main

import (
    "encoding/base64"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "strings"
    "time"
)

// WebhookServer handles HTTP endpoints
type WebhookServer struct {
    db     *Database
    config *Config
}

// NewWebhookServer creates a new webhook server instance
func NewWebhookServer(db *Database, config *Config) *WebhookServer {
    return &WebhookServer{
        db:     db,
        config: config,
    }
}

// Start begins listening for HTTP requests
func (s *WebhookServer) Start() error {
    http.HandleFunc("/health", s.loggingMiddleware(s.healthHandler))
    http.HandleFunc("/stats", s.loggingMiddleware(s.statsHandler))
    http.HandleFunc("/webhook", s.loggingMiddleware(s.webhookHandler))
    http.HandleFunc("/members", s.loggingMiddleware(s.listMembersHandler))
    
    addr := "127.0.0.1:" + s.config.Port
    logger.Printf("Starting membership server on %s", addr)
    logger.Printf("Webhook endpoint: https://memberships.operatorfoundation.org/webhook")
    logger.Printf("Stats endpoint: https://memberships.operatorfoundation.org/stats")
    logger.Printf("Members endpoint: https://memberships.operatorfoundation.org/members")
    
    return http.ListenAndServe(addr, nil)
}

// loggingMiddleware logs all HTTP requests
func (s *WebhookServer) loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        logger.Printf("%s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
        next(w, r)
        logger.Printf("Request completed in %v", time.Since(start))
    }
}

// healthHandler returns server health status
func (s *WebhookServer) healthHandler(w http.ResponseWriter, r *http.Request) {
    dbStatus := "ok"
    if err := s.db.HealthCheck(); err != nil {
        dbStatus = fmt.Sprintf("error: %v", err)
    }
    
    response := map[string]interface{}{
        "status":    "ok",
        "timestamp": time.Now().Format(time.RFC3339),
        "database":  dbStatus,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// statsHandler returns membership statistics
func (s *WebhookServer) statsHandler(w http.ResponseWriter, r *http.Request) {
    stats, err := s.db.GetStats()
    if err != nil {
        logger.Printf("Error getting stats: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}

// webhookHandler processes incoming webhooks from Zapier
func (s *WebhookServer) webhookHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    // Check authorization
    if !s.isAuthorized(r) {
        logger.Printf("Unauthorized webhook attempt from %s", r.RemoteAddr)
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }
    
    // Read body
    body, err := io.ReadAll(r.Body)
    if err != nil {
        logger.Printf("Error reading body: %v", err)
        http.Error(w, "Bad request", http.StatusBadRequest)
        return
    }
    defer r.Body.Close()
    
    // Parse webhook
    var webhook MemberWebhook
    if err := json.Unmarshal(body, &webhook); err != nil {
        logger.Printf("Error parsing JSON: %v", err)
        logger.Printf("Raw body: %s", string(body))
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    logger.Printf("Webhook received - Email: %s, Status: %s, Anonymous: %s", 
        webhook.Email, webhook.Status, webhook.Anonymous)
    
    // Process the webhook
    status := s.convertStatus(webhook.Status)
    isAnonymous := s.convertAnonymous(webhook.Anonymous)
    
    // Log webhook for debugging
    if err := s.db.LogWebhook(webhook.Email, status, body); err != nil {
        logger.Printf("Warning: Failed to log webhook: %v", err)
    }
    
    // Process member
    if err := s.db.ProcessMember(webhook.Email, webhook.Name, isAnonymous, status); err != nil {
        logger.Printf("Error processing member: %v", err)
        // Still return 200 to prevent retries
    }
    
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "OK")
}

// listMembersHandler returns a list of members
func (s *WebhookServer) listMembersHandler(w http.ResponseWriter, r *http.Request) {
    status := r.URL.Query().Get("status")
    
    members, err := s.db.GetMembers(status, 100)
    if err != nil {
        logger.Printf("Error getting members: %v", err)
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(members)
}

// isAuthorized checks if the request has valid authentication
func (s *WebhookServer) isAuthorized(r *http.Request) bool {
    authHeader := r.Header.Get("Authorization")
    
    // Check Bearer token
    if authHeader == "Bearer " + s.config.WebhookSecret {
        return true
    }
    
    // Check Basic auth (from Zapier)
    if strings.HasPrefix(authHeader, "Basic ") {
        payload, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader, "Basic "))
        parts := strings.SplitN(string(payload), ":", 2)
        if len(parts) == 2 && (parts[1] == s.config.WebhookSecret || parts[0] == s.config.WebhookSecret) {
            return true
        }
    }
    
    // Check custom header
    if r.Header.Get("X-Webhook-Secret") == s.config.WebhookSecret {
        return true
    }
    
    return false
}

// convertStatus converts Zapier's payment status to membership status
func (s *WebhookServer) convertStatus(zapierStatus string) string {
    statusLower := strings.ToLower(zapierStatus)
    
    if strings.Contains(statusLower, "succeed") || strings.Contains(statusLower, "success") || strings.Contains(statusLower, "active") {
        return "active"
    } else if strings.Contains(statusLower, "fail") || strings.Contains(statusLower, "cancel") || strings.Contains(statusLower, "refund") {
        return "cancelled"
    } else if strings.Contains(statusLower, "suspend") || strings.Contains(statusLower, "pend") {
        return "suspended"
    }
    
    logger.Printf("Unexpected status '%s', defaulting to 'active'", zapierStatus)
    return "active"
}

// convertAnonymous converts Zapier's anonymous string to boolean
func (s *WebhookServer) convertAnonymous(anonStr string) bool {
    anonLower := strings.ToLower(strings.TrimSpace(anonStr))
    return anonLower == "true" || anonLower == "yes" || anonLower == "1"
}
