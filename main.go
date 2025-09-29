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
    DatabaseURL    string
    Port           string
    WebhookSecret  string // Shared secret with Zapier for authentication
}

// Webhook payload from Zapier/GiveLively
type MemberWebhook struct {
    Email        string  `json:"email"`
    Name         string  `json:"name"`
    Amount       float64 `json:"amount"`
    Frequency    string  `json:"frequency"`
    Status       string  `json:"status"`        // active, cancelled, failed
    DonationID   string  `json:"donation_id"`   // GiveLively's donation/subscription ID
    DonationDate string  `json:"donation_date"`
    EventType    string  `json:"event_type"`    // new_subscription, cancelled, payment, etc.
    Campaign     string  `json:"campaign,omitempty"`
    Phone        string  `json:"phone,omitempty"`
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
    -- Members table (simplified - no PayPal specific fields)
    CREATE TABLE IF NOT EXISTS members (
        id SERIAL PRIMARY KEY,
        email VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(255),
        subscription_id VARCHAR(100) UNIQUE,  -- GiveLively donation ID
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        monthly_amount DECIMAL(10, 2),
        frequency VARCHAR(20) DEFAULT 'monthly',
        campaign VARCHAR(255),
        phone VARCHAR(50),
        joined_date DATE,
        cancelled_date DATE,
        last_payment_date DATE,
        total_donated DECIMAL(10, 2) DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Payments table (simplified)
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        member_id INTEGER REFERENCES members(id) ON DELETE CASCADE,
        amount DECIMAL(10, 2),
        payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        donation_id VARCHAR(100),
        notes TEXT
    );

    -- Activity log for debugging
    CREATE TABLE IF NOT EXISTS webhook_logs (
        id SERIAL PRIMARY KEY,
        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        event_type VARCHAR(50),
        email VARCHAR(255),
        payload JSONB,
        processed BOOLEAN DEFAULT true
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_members_email ON members(email);
    CREATE INDEX IF NOT EXISTS idx_members_status ON members(status);
    CREATE INDEX IF NOT EXISTS idx_members_subscription_id ON members(subscription_id);
    CREATE INDEX IF NOT EXISTS idx_payments_member_id ON payments(member_id);
    CREATE INDEX IF NOT EXISTS idx_webhook_logs_email ON webhook_logs(email);
    `
    
    _, err := db.Exec(schema)
    if err != nil {
        return fmt.Errorf("failed to create tables: %w", err)
    }
    
    logger.Println("Database tables verified/created successfully")
    return nil
}

// Process webhook based on event type
func (db *Database) processWebhook(webhook MemberWebhook) error {
    // Log the webhook for debugging
    payloadJSON, _ := json.Marshal(webhook)
    _, err := db.Exec(`
        INSERT INTO webhook_logs (event_type, email, payload)
        VALUES ($1, $2, $3)
    `, webhook.EventType, webhook.Email, payloadJSON)
    
    if err != nil {
        logger.Printf("Warning: Failed to log webhook: %v", err)
    }
    
    // Process based on event type
    switch webhook.EventType {
    case "new_subscription", "new_recurring":
        return db.handleNewSubscription(webhook)
    case "cancelled", "subscription_cancelled":
        return db.handleCancellation(webhook)
    case "payment", "payment_received":
        return db.handlePayment(webhook)
    case "failed", "payment_failed":
        return db.handleFailedPayment(webhook)
    default:
        logger.Printf("Unknown event type: %s", webhook.EventType)
        return nil
    }
}

// Handle new subscription
func (db *Database) handleNewSubscription(webhook MemberWebhook) error {
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    logger.Printf("Processing new subscription for %s", email)
    
    // Check if member exists
    var memberID int
    err := db.QueryRow(`
        SELECT id FROM members WHERE email = $1
    `, email).Scan(&memberID)
    
    if err == sql.ErrNoRows {
        // Create new member
        err = db.QueryRow(`
            INSERT INTO members (
                email, name, subscription_id, status, 
                monthly_amount, frequency, campaign, phone,
                joined_date, last_payment_date
            ) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7, CURRENT_DATE, CURRENT_DATE)
            RETURNING id
        `, email, webhook.Name, webhook.DonationID, 
           webhook.Amount, webhook.Frequency, webhook.Campaign, webhook.Phone).Scan(&memberID)
        
        if err != nil {
            return fmt.Errorf("failed to create member: %w", err)
        }
        
        logger.Printf("Created new member: %s (ID: %d)", email, memberID)
        
    } else if err == nil {
        // Update existing member
        _, err = db.Exec(`
            UPDATE members SET
                name = COALESCE(NULLIF($1, ''), name),
                subscription_id = COALESCE(NULLIF($2, ''), subscription_id),
                status = 'active',
                monthly_amount = $3,
                frequency = COALESCE(NULLIF($4, ''), frequency),
                campaign = COALESCE(NULLIF($5, ''), campaign),
                phone = COALESCE(NULLIF($6, ''), phone),
                cancelled_date = NULL,
                last_payment_date = CURRENT_DATE,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $7
        `, webhook.Name, webhook.DonationID, webhook.Amount, 
           webhook.Frequency, webhook.Campaign, webhook.Phone, memberID)
        
        if err != nil {
            return fmt.Errorf("failed to update member: %w", err)
        }
        
        logger.Printf("Reactivated existing member: %s (ID: %d)", email, memberID)
    } else {
        return fmt.Errorf("database error: %w", err)
    }
    
    // Record the payment
    _, err = db.Exec(`
        INSERT INTO payments (member_id, amount, donation_id, notes)
        VALUES ($1, $2, $3, 'Initial subscription payment')
    `, memberID, webhook.Amount, webhook.DonationID)
    
    if err != nil {
        logger.Printf("Warning: Failed to record payment: %v", err)
    }
    
    // Update total donated
    _, err = db.Exec(`
        UPDATE members 
        SET total_donated = total_donated + $1
        WHERE id = $2
    `, webhook.Amount, memberID)
    
    return nil
}

// Handle subscription cancellation
func (db *Database) handleCancellation(webhook MemberWebhook) error {
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    logger.Printf("Processing cancellation for %s", email)
    
    result, err := db.Exec(`
        UPDATE members SET
            status = 'cancelled',
            cancelled_date = CURRENT_DATE,
            updated_at = CURRENT_TIMESTAMP
        WHERE email = $1 OR subscription_id = $2
    `, email, webhook.DonationID)
    
    if err != nil {
        return fmt.Errorf("failed to cancel subscription: %w", err)
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        logger.Printf("Warning: No member found for cancellation: %s", email)
    } else {
        logger.Printf("Cancelled subscription for %s", email)
    }
    
    return nil
}

// Handle successful payment
func (db *Database) handlePayment(webhook MemberWebhook) error {
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    logger.Printf("Processing payment from %s: $%.2f", email, webhook.Amount)
    
    // Get member ID
    var memberID int
    err := db.QueryRow(`
        SELECT id FROM members 
        WHERE email = $1 OR subscription_id = $2
    `, email, webhook.DonationID).Scan(&memberID)
    
    if err != nil {
        logger.Printf("Warning: Member not found for payment: %s", email)
        return nil
    }
    
    // Record payment
    _, err = db.Exec(`
        INSERT INTO payments (member_id, amount, donation_id, notes)
        VALUES ($1, $2, $3, 'Recurring payment')
    `, memberID, webhook.Amount, webhook.DonationID)
    
    if err != nil {
        return fmt.Errorf("failed to record payment: %w", err)
    }
    
    // Update member
    _, err = db.Exec(`
        UPDATE members SET
            last_payment_date = CURRENT_DATE,
            total_donated = total_donated + $1,
            status = 'active',
            updated_at = CURRENT_TIMESTAMP
        WHERE id = $2
    `, webhook.Amount, memberID)
    
    return err
}

// Handle failed payment
func (db *Database) handleFailedPayment(webhook MemberWebhook) error {
    email := strings.ToLower(strings.TrimSpace(webhook.Email))
    
    logger.Printf("Processing failed payment for %s", email)
    
    _, err := db.Exec(`
        UPDATE members SET
            status = 'payment_failed',
            updated_at = CURRENT_TIMESTAMP
        WHERE email = $1 OR subscription_id = $2
    `, email, webhook.DonationID)
    
    return err
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
        TotalMembers    int     `json:"total_members"`
        ActiveMembers   int     `json:"active_members"`
        MonthlyRevenue  float64 `json:"monthly_revenue"`
        TotalDonated    float64 `json:"total_donated"`
        ChurnedThisMonth int    `json:"churned_this_month"`
    }
    
    // Get stats from database
    db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&stats.TotalMembers)
    db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'active'`).Scan(&stats.ActiveMembers)
    db.QueryRow(`SELECT COALESCE(SUM(monthly_amount), 0) FROM members WHERE status = 'active'`).Scan(&stats.MonthlyRevenue)
    db.QueryRow(`SELECT COALESCE(SUM(total_donated), 0) FROM members`).Scan(&stats.TotalDonated)
    db.QueryRow(`
        SELECT COUNT(*) FROM members 
        WHERE cancelled_date >= date_trunc('month', CURRENT_DATE)
    `).Scan(&stats.ChurnedThisMonth)
    
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
    logger.Printf("Webhook received - Type: %s, Email: %s, Amount: %.2f", 
        webhook.EventType, webhook.Email, webhook.Amount)
    
    // Process webhook
    if err := db.processWebhook(webhook); err != nil {
        logger.Printf("Error processing webhook: %v", err)
        // Still return 200 to prevent retries
    }
    
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "OK")
}

// List members endpoint (useful for debugging)
func listMembersHandler(w http.ResponseWriter, r *http.Request) {
    // Optional: Add authentication here too
    
    rows, err := db.Query(`
        SELECT email, name, status, monthly_amount, joined_date, total_donated
        FROM members
        ORDER BY created_at DESC
        LIMIT 100
    `)
    if err != nil {
        http.Error(w, "Database error", http.StatusInternalServerError)
        return
    }
    defer rows.Close()
    
    var members []map[string]interface{}
    for rows.Next() {
        var email, name, status sql.NullString
        var amount, total sql.NullFloat64
        var joined sql.NullTime
        
        rows.Scan(&email, &name, &status, &amount, &joined, &total)
        
        members = append(members, map[string]interface{}{
            "email":         email.String,
            "name":          name.String,
            "status":        status.String,
            "monthly_amount": amount.Float64,
            "joined_date":   joined.Time,
            "total_donated": total.Float64,
        })
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
    
    // Create tables
    if err := db.createTables(); err != nil {
        logger.Fatalf("Failed to create tables: %v", err)
    }
    
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
    
    if err := http.ListenAndServe(addr, nil); err != nil {
        logger.Fatalf("Server failed: %v", err)
    }
}
