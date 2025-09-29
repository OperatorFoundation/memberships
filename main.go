package main

import (
    "database/sql"
    "encoding/base64"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strings"
    "time"

    "github.com/joho/godotenv"
    _ "github.com/lib/pq"
)

// Configuration
type Config struct {
    DatabaseURL   string
    Port          string
    WebhookSecret string
}

// Webhook payload from Zapier - accepts strings since that's what Zapier sends
type MemberWebhook struct {
    Email     string `json:"email"`
    Name      string `json:"name"`
    Status    string `json:"status"`    // Zapier sends "Succeeded", "Failed", etc.
    Anonymous string `json:"anonymous"` // Zapier sends "True", "False" as strings
}

// Database wrapper
type Database struct {
    *sql.DB
}

var (
    config Config
    logger *log.Logger
    db     *Database
)

func init() {
    // Set up logger
    logger = log.New(os.Stdout, "[MEMBERSHIP] ", log.LstdFlags|log.Lshortfile)
    
    // Load .env file if it exists
    if err := godotenv.Load(); err != nil {
        logger.Println("No .env file found")
    }
    
    // Load configuration
    config = Config{
        DatabaseURL:   os.Getenv("DATABASE_URL"),
        Port:          getEnvOrDefault("PORT", "3000"),
        WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
    }
    
    // Validate required config
    if config.DatabaseURL == "" {
        logger.Fatal("DATABASE_URL environment variable is required")
    }
    
    if config.WebhookSecret == "" {
        logger.Fatal("WEBHOOK_SECRET environment variable is required")
    }
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

// Initialize database connection
func NewDatabase(connStr string) (*Database, error) {
    conn, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, fmt.Errorf("failed to open database: %w", err)
    }
    
    // Configure connection pool
    conn.SetMaxOpenConns(10)
    conn.SetMaxIdleConns(5)
    conn.SetConnMaxLifetime(5 * time.Minute)
    
    // Test connection
    if err := conn.Ping(); err != nil {
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }
    
    logger.Println("Database connected successfully")
    return &Database{conn}, nil
}

// Process incoming webhook
func (db *Database) processWebhook(webhook MemberWebhook) error {
    // Normalize email
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    // Validate email
    if email == "" {
        return fmt.Errorf("email is required")
    }
    
    // Convert Zapier's payment status to our membership status
    memberStatus := "active" // default
    statusLower := strings.ToLower(webhook.Status)
    
    if strings.Contains(statusLower, "succeed") || strings.Contains(statusLower, "success") || strings.Contains(statusLower, "active") {
        memberStatus = "active"
    } else if strings.Contains(statusLower, "fail") || strings.Contains(statusLower, "cancel") || strings.Contains(statusLower, "refund") {
        memberStatus = "cancelled"
    } else if strings.Contains(statusLower, "suspend") || strings.Contains(statusLower, "pend") {
        memberStatus = "suspended"
    } else {
        logger.Printf("Unexpected status '%s' for %s, defaulting to 'active'", webhook.Status, email)
    }
    
    // Convert anonymous string to boolean
    isAnonymous := false
    anonLower := strings.ToLower(strings.TrimSpace(webhook.Anonymous))
    if anonLower == "true" || anonLower == "yes" || anonLower == "1" {
        isAnonymous = true
    }
    
    // Determine name to store
    nameToStore := webhook.Name
    if isAnonymous {
        nameToStore = "" // Don't store name for anonymous donations
    }
    
    // Log the webhook
    payloadJSON, _ := json.Marshal(webhook)
    _, err := db.Exec(`
        INSERT INTO webhook_logs (email, status, payload)
        VALUES ($1, $2, $3)
    `, email, memberStatus, payloadJSON)
    
    if err != nil {
        logger.Printf("Warning: Failed to log webhook: %v", err)
    }
    
    // Check if member exists
    var memberID int
    var currentStatus string
    err = db.QueryRow(`
        SELECT id, status FROM members WHERE email = $1
    `, email).Scan(&memberID, &currentStatus)
    
    if err == sql.ErrNoRows {
        // Create new member
        err = db.QueryRow(`
            INSERT INTO members (email, name, is_anonymous, status, first_seen, last_updated)
            VALUES ($1, $2, $3, $4, CURRENT_DATE, CURRENT_TIMESTAMP)
            RETURNING id
        `, email, nameToStore, isAnonymous, memberStatus).Scan(&memberID)
        
        if err != nil {
            return fmt.Errorf("failed to create member: %w", err)
        }
        
        logger.Printf("Created new member: %s (ID: %d, Status: %s)", email, memberID, memberStatus)
        
        // Record initial status in history
        _, err = db.Exec(`
            INSERT INTO status_history (member_id, status)
            VALUES ($1, $2)
        `, memberID, memberStatus)
        
    } else if err == nil {
        // Update existing member
        _, err = db.Exec(`
            UPDATE members SET
                name = CASE 
                    WHEN $1 = true THEN name  -- Keep existing name if anonymous
                    WHEN $2 = '' THEN name     -- Keep existing name if new name is empty
                    ELSE $2                    -- Otherwise update name
                END,
                is_anonymous = $1,
                status = $3,
                last_updated = CURRENT_TIMESTAMP
            WHERE id = $4
        `, isAnonymous, nameToStore, memberStatus, memberID)
        
        if err != nil {
            return fmt.Errorf("failed to update member: %w", err)
        }
        
        // Record status change if different
        if currentStatus != memberStatus {
            _, err = db.Exec(`
                INSERT INTO status_history (member_id, status)
                VALUES ($1, $2)
            `, memberID, memberStatus)
            
            logger.Printf("Updated member %s (ID: %d): %s -> %s", 
                email, memberID, currentStatus, memberStatus)
        } else {
            logger.Printf("Member %s (ID: %d) status unchanged: %s", 
                email, memberID, memberStatus)
        }
        
    } else {
        return fmt.Errorf("database error: %w", err)
    }
    
    return nil
}

// Health check endpoint
func healthHandler(w http.ResponseWriter, r *http.Request) {
    dbStatus := "ok"
    if err := db.Ping(); err != nil {
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

// Stats endpoint
func statsHandler(w http.ResponseWriter, r *http.Request) {
    var stats struct {
        TotalMembers     int `json:"total_members"`
        ActiveMembers    int `json:"active_members"`
        CancelledMembers int `json:"cancelled_members"`
        AnonymousMembers int `json:"anonymous_members"`
    }
    
    db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&stats.TotalMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'active'`).Scan(&stats.ActiveMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'cancelled'`).Scan(&stats.CancelledMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE is_anonymous = true`).Scan(&stats.AnonymousMembers)
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}

// Webhook handler
func webhookHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    // Check authorization - support multiple formats
    authHeader := r.Header.Get("Authorization")
    authorized := false
    
    // Check Bearer token
    if authHeader == "Bearer " + config.WebhookSecret {
        authorized = true
    }
    
    // Check Basic auth (from Zapier)
    if strings.HasPrefix(authHeader, "Basic ") {
        payload, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(authHeader, "Basic "))
        parts := strings.SplitN(string(payload), ":", 2)
        if len(parts) == 2 && (parts[1] == config.WebhookSecret || parts[0] == config.WebhookSecret) {
            authorized = true
        }
    }
    
    // Check custom header as fallback
    if !authorized && r.Header.Get("X-Webhook-Secret") == config.WebhookSecret {
        authorized = true
    }
    
    if !authorized {
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
    
    // Log received webhook
    logger.Printf("Webhook received - Email: %s, Status: %s, Anonymous: %s", 
        webhook.Email, webhook.Status, webhook.Anonymous)
    
    // Process webhook
    if err := db.processWebhook(webhook); err != nil {
        logger.Printf("Error processing webhook: %v", err)
        // Still return 200 to prevent retries
    }
    
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "OK")
}

// List members endpoint
func listMembersHandler(w http.ResponseWriter, r *http.Request) {
    status := r.URL.Query().Get("status")
    
    query := `
        SELECT email, name, is_anonymous, status, first_seen, last_updated
        FROM members
    `
    args := []interface{}{}
    
    if status != "" {
        query += " WHERE status = $1"
        args = append(args, status)
    }
    
    query += " ORDER BY last_updated DESC LIMIT 100"
    
    rows, err := db.Query(query, args...)
    if err != nil {
        http.Error(w, "Database error", http.StatusInternalServerError)
        return
    }
    defer rows.Close()
    
    var members []map[string]interface{}
    for rows.Next() {
        var email, name, status sql.NullString
        var isAnonymous sql.NullBool
        var firstSeen, lastUpdated sql.NullTime
        
        rows.Scan(&email, &name, &isAnonymous, &status, &firstSeen, &lastUpdated)
        
        member := map[string]interface{}{
            "email":        email.String,
            "status":       status.String,
            "is_anonymous": isAnonymous.Bool,
            "first_seen":   firstSeen.Time,
            "last_updated": lastUpdated.Time,
        }
        
        if !isAnonymous.Bool && name.Valid {
            member["name"] = name.String
        }
        
        members = append(members, member)
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(members)
}

// Logging middleware
func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()
        logger.Printf("%s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
        next(w, r)
        logger.Printf("Request completed in %v", time.Since(start))
    }
}

func main() {
    // Initialize database
    var err error
    db, err = NewDatabase(config.DatabaseURL)
    if err != nil {
        logger.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
    
    // Note: Tables should already exist from migrations
    // If you need to create tables, run migrations first
    
    // Set up routes
    http.HandleFunc("/health", loggingMiddleware(healthHandler))
    http.HandleFunc("/stats", loggingMiddleware(statsHandler))
    http.HandleFunc("/webhook", loggingMiddleware(webhookHandler))
    http.HandleFunc("/members", loggingMiddleware(listMembersHandler))
    
    // Start server
    addr := "127.0.0.1:" + config.Port
    logger.Printf("Starting membership server on %s", addr)
    logger.Printf("Webhook endpoint: https://memberships.operatorfoundation.org/webhook")
    logger.Printf("Stats endpoint: https://memberships.operatorfoundation.org/stats")
    logger.Printf("Members endpoint: https://memberships.operatorfoundation.org/members")
    
    if err := http.ListenAndServe(addr, nil); err != nil {
        logger.Fatalf("Server failed: %v", err)
    }
}
