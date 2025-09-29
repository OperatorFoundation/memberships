package main

import (
    "database/sql"
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
    WebhookSecret string // Shared secret with Zapier for authentication
}

// Webhook payload from Zapier/GiveLively
type MemberWebhook struct {
    Email     string `json:"email"`
    Name      string `json:"name"`
    Status    string `json:"status"`    // active, cancelled, suspended
    Anonymous bool   `json:"anonymous"` // If true, don't store name
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

// Create tables if they don't exist
func (db *Database) createTables() error {
    schema := `
    -- Members table (simplified - email is the identifier)
    CREATE TABLE IF NOT EXISTS members (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(255),
        is_anonymous BOOLEAN DEFAULT false,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        first_seen DATE DEFAULT CURRENT_DATE,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Status history for tracking changes
    CREATE TABLE IF NOT EXISTS status_history (
        id SERIAL PRIMARY KEY,
        member_id INTEGER REFERENCES members(id) ON DELETE CASCADE,
        status VARCHAR(20) NOT NULL,
        changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Webhook log for debugging
    CREATE TABLE IF NOT EXISTS webhook_logs (
        id SERIAL PRIMARY KEY,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        email VARCHAR(255),
        status VARCHAR(20),
        payload JSONB
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_members_email ON members(email);
    CREATE INDEX IF NOT EXISTS idx_members_status ON members(status);
    CREATE INDEX IF NOT EXISTS idx_status_history_member_id ON status_history(member_id);
    `
    
    _, err := db.Exec(schema)
    if err != nil {
        return fmt.Errorf("failed to create tables: %w", err)
    }
    
    logger.Println("Database tables verified/created successfully")
    return nil
}

// Process incoming webhook
func (db *Database) processWebhook(webhook MemberWebhook) error {
    // Normalize email
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    // Validate email
    if email == "" {
        return fmt.Errorf("email is required")
    }
    
    // Log the webhook
    payloadJSON, _ := json.Marshal(webhook)
    _, err := db.Exec(`
        INSERT INTO webhook_logs (email, status, payload)
        VALUES ($1, $2, $3)
    `, email, webhook.Status, payloadJSON)
    
    if err != nil {
        logger.Printf("Warning: Failed to log webhook: %v", err)
    }
    
    // Determine name to store
    nameToStore := webhook.Name
    if webhook.Anonymous {
        nameToStore = "" // Don't store name for anonymous donations
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
            INSERT INTO members (email, name, is_anonymous, status)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        `, email, nameToStore, webhook.Anonymous, webhook.Status).Scan(&memberID)
        
        if err != nil {
            return fmt.Errorf("failed to create member: %w", err)
        }
        
        logger.Printf("Created new member: %s (ID: %d, Status: %s)", email, memberID, webhook.Status)
        
        // Record initial status in history
        _, err = db.Exec(`
            INSERT INTO status_history (member_id, status)
            VALUES ($1, $2)
        `, memberID, webhook.Status)
        
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
        `, webhook.Anonymous, nameToStore, webhook.Status, memberID)
        
        if err != nil {
            return fmt.Errorf("failed to update member: %w", err)
        }
        
        // Record status change if it's different
        if currentStatus != webhook.Status {
            _, err = db.Exec(`
                INSERT INTO status_history (member_id, status)
                VALUES ($1, $2)
            `, memberID, webhook.Status)
            
            logger.Printf("Updated member %s (ID: %d): %s -> %s", 
                email, memberID, currentStatus, webhook.Status)
        } else {
            logger.Printf("Member %s (ID: %d) status unchanged: %s", 
                email, memberID, webhook.Status)
        }
        
    } else {
        return fmt.Errorf("database error: %w", err)
    }
    
    return nil
}

// HTTP Handlers

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
    
    // Get stats from database
    db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&stats.TotalMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'active'`).Scan(&stats.ActiveMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'cancelled'`).Scan(&stats.CancelledMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE is_anonymous = true`).Scan(&stats.AnonymousMembers)
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}

// Webhook handler
func webhookHandler(w http.ResponseWriter, r *http.Request) {
    // Verify method
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    // Verify authentication
    authHeader := r.Header.Get("Authorization")
    expectedAuth := "Bearer " + config.WebhookSecret
    
    if authHeader != expectedAuth {
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
    logger.Printf("Webhook received - Email: %s, Status: %s, Anonymous: %v", 
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
    // Optional: Add authentication here
    
    status := r.URL.Query().Get("status") // Allow filtering by status
    
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
        
        // Only include name if not anonymous
        if !isAnonymous.Bool && name.Valid {
            member["name"] = name.String
        }
        
        members = append(members, member)
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(members)
}

// Test webhook endpoint for debugging
func testWebhookHandler(w http.ResponseWriter, r *http.Request) {
    testData := MemberWebhook{
        Email:     "test@example.com",
        Name:      "Test User",
        Status:    "active",
        Anonymous: false,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(testData)
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
    
    // Create tables
    if err := db.createTables(); err != nil {
        logger.Fatalf("Failed to create tables: %v", err)
    }
    
    // Set up routes
    http.HandleFunc("/health", loggingMiddleware(healthHandler))
    http.HandleFunc("/stats", loggingMiddleware(statsHandler))
    http.HandleFunc("/webhook", loggingMiddleware(webhookHandler))
    http.HandleFunc("/members", loggingMiddleware(listMembersHandler))
    http.HandleFunc("/test", loggingMiddleware(testWebhookHandler))
    
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
