package main

import (
    "bytes"
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

// PayPal webhook event structures
type WebhookEvent struct {
    ID         string    `json:"id"`
    EventType  string    `json:"event_type"`
    CreateTime time.Time `json:"create_time"`
    Resource   Resource  `json:"resource"`
}

type Resource struct {
    ID                 string     `json:"id"`
    Status             string     `json:"status"`
    CustomID           string     `json:"custom_id"`
    Subscriber         Subscriber `json:"subscriber"`
    BillingAgreementID string     `json:"billing_agreement_id"`
    Amount             Amount     `json:"amount"`
    Payer              Payer      `json:"payer"`
}

type Subscriber struct {
    EmailAddress string `json:"email_address"`
    PayerID      string `json:"payer_id"`
    Name         Name   `json:"name"`
}

type Payer struct {
    PayerID      string       `json:"payer_id"`
    EmailAddress string       `json:"email_address"`
    PayerInfo    PayerInfo    `json:"payer_info"`
}

type PayerInfo struct {
    Email   string `json:"email"`
    PayerID string `json:"payer_id"`
}

type Name struct {
    GivenName string `json:"given_name"`
    Surname   string `json:"surname"`
}

type Amount struct {
    Value        string `json:"value"`
    CurrencyCode string `json:"currency_code"`
}

// Configuration
type Config struct {
    PayPalClientID     string
    PayPalClientSecret string
    PayPalWebhookID    string
    PayPalBaseURL      string
    DatabaseURL        string
    Port               string
}

// Database wrapper
type Database struct {
    *sql.DB
}

var config Config
var logger *log.Logger
var db *Database

func init() {
    // Set up logger
    logger = log.New(os.Stdout, "[WEBHOOK] ", log.LstdFlags|log.Lshortfile)
    
    // Load .env file if it exists
    if err := godotenv.Load(); err != nil {
        logger.Println("No .env file found")
    }
    
    // Load configuration
    config = Config{
        PayPalClientID:     os.Getenv("PAYPAL_CLIENT_ID"),
        PayPalClientSecret: os.Getenv("PAYPAL_CLIENT_SECRET"),
        PayPalWebhookID:    os.Getenv("PAYPAL_WEBHOOK_ID"),
        PayPalBaseURL:      getEnvOrDefault("PAYPAL_BASE_URL", "https://api.paypal.com"),
        DatabaseURL:        os.Getenv("DATABASE_URL"),
        Port:               getEnvOrDefault("PORT", "3000"),
    }
    
    if config.DatabaseURL == "" {
        logger.Fatal("DATABASE_URL environment variable is required")
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
    conn.SetMaxOpenConns(25)
    conn.SetMaxIdleConns(5)
    conn.SetConnMaxLifetime(5 * time.Minute)
    
    // Test connection
    if err := conn.Ping(); err != nil {
        return nil, fmt.Errorf("failed to ping database: %w", err)
    }
    
    return &Database{conn}, nil
}

// Create tables if they don't exist
func (db *Database) createTables() error {
    schema := `
    -- Members table
    CREATE TABLE IF NOT EXISTS members (
        id SERIAL PRIMARY KEY,
        paypal_email VARCHAR(255) UNIQUE NOT NULL,
        paypal_payer_id VARCHAR(50) UNIQUE,
        current_subscription_id VARCHAR(50) UNIQUE,
        subscription_status VARCHAR(20) NOT NULL DEFAULT 'pending',
        monthly_amount DECIMAL(10, 2),
        referred_by VARCHAR(255),
        given_name VARCHAR(100),
        surname VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Subscription history table
    CREATE TABLE IF NOT EXISTS subscription_history (
        id SERIAL PRIMARY KEY,
        member_id INTEGER REFERENCES members(id),
        subscription_id VARCHAR(50) NOT NULL,
        status VARCHAR(20),
        started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        ended_at TIMESTAMP,
        UNIQUE(subscription_id)
    );

    -- Payments table
    CREATE TABLE IF NOT EXISTS payments (
        id SERIAL PRIMARY KEY,
        member_id INTEGER REFERENCES members(id),
        subscription_id VARCHAR(50),
        transaction_id VARCHAR(50) UNIQUE NOT NULL,
        amount DECIMAL(10, 2),
        currency VARCHAR(3),
        payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(20)
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_subscription_status ON members(subscription_status);
    CREATE INDEX IF NOT EXISTS idx_referred_by ON members(referred_by);
    CREATE INDEX IF NOT EXISTS idx_member_payments ON payments(member_id);
    CREATE INDEX IF NOT EXISTS idx_subscription_payments ON payments(subscription_id);
    `
    
    _, err := db.Exec(schema)
    if err != nil {
        return fmt.Errorf("failed to create tables: %w", err)
    }
    
    logger.Println("Database tables created/verified successfully")
    return nil
}

// Find or create member based on subscription activation
func (db *Database) handleSubscriptionActivated(event WebhookEvent) error {
    email := strings.ToLower(event.Resource.Subscriber.EmailAddress)
    subscriptionID := event.Resource.ID
    payerID := event.Resource.Subscriber.PayerID
    
    // Extract referrer from custom_id if present
    var referrer string
    if event.Resource.CustomID != "" {
        // Assuming format like "referrer:email@example.com"
        parts := strings.SplitN(event.Resource.CustomID, ":", 2)
        if len(parts) == 2 && parts[0] == "referrer" {
            referrer = parts[1]
        }
    }
    
    // Parse amount - default to 10 if not specified
    amount := "10.00"
    if event.Resource.Amount.Value != "" {
        amount = event.Resource.Amount.Value
    }
    
    // Try to find existing member by email or payer_id
    var memberID int
    var exists bool
    
    err := db.QueryRow(`
        SELECT id FROM members 
        WHERE paypal_email = $1 OR 
              (paypal_payer_id = $2 AND $2 != '' AND $2 IS NOT NULL)
        LIMIT 1
    `, email, payerID).Scan(&memberID)
    
    exists = err == nil
    
    if exists {
        // Update existing member
        logger.Printf("Updating existing member: %s", email)
        
        updateQuery := `
            UPDATE members SET
                current_subscription_id = $1,
                subscription_status = 'active',
                monthly_amount = $2,
                paypal_payer_id = COALESCE(NULLIF($3, ''), paypal_payer_id),
                given_name = COALESCE(NULLIF($4, ''), given_name),
                surname = COALESCE(NULLIF($5, ''), surname),
                updated_at = CURRENT_TIMESTAMP
            WHERE id = $6
        `
        
        _, err = db.Exec(updateQuery,
            subscriptionID,
            amount,
            payerID,
            event.Resource.Subscriber.Name.GivenName,
            event.Resource.Subscriber.Name.Surname,
            memberID,
        )
        
        if err != nil {
            return fmt.Errorf("failed to update member: %w", err)
        }
        
    } else {
        // Create new member
        logger.Printf("Creating new member: %s", email)
        
        insertQuery := `
            INSERT INTO members (
                paypal_email,
                paypal_payer_id,
                current_subscription_id,
                subscription_status,
                monthly_amount,
                referred_by,
                given_name,
                surname
            ) VALUES ($1, $2, $3, 'active', $4, $5, $6, $7)
            RETURNING id
        `
        
        err = db.QueryRow(insertQuery,
            email,
            payerID,
            subscriptionID,
            amount,
            referrer,
            event.Resource.Subscriber.Name.GivenName,
            event.Resource.Subscriber.Name.Surname,
        ).Scan(&memberID)
        
        if err != nil {
            return fmt.Errorf("failed to create member: %w", err)
        }
    }
    
    // Record in subscription history
    _, err = db.Exec(`
        INSERT INTO subscription_history (member_id, subscription_id, status)
        VALUES ($1, $2, 'active')
        ON CONFLICT (subscription_id) DO UPDATE 
        SET status = 'active', ended_at = NULL
    `, memberID, subscriptionID)
    
    if err != nil {
        logger.Printf("Warning: Failed to record subscription history: %v", err)
    }
    
    logger.Printf("Successfully processed subscription activation for %s (member_id: %d)", email, memberID)
    return nil
}

// Update member status when subscription is cancelled
func (db *Database) handleSubscriptionCancelled(event WebhookEvent) error {
    subscriptionID := event.Resource.ID
    
    result, err := db.Exec(`
        UPDATE members 
        SET subscription_status = 'cancelled', 
            updated_at = CURRENT_TIMESTAMP
        WHERE current_subscription_id = $1
    `, subscriptionID)
    
    if err != nil {
        return fmt.Errorf("failed to update cancelled subscription: %w", err)
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        logger.Printf("Warning: Subscription %s not found in database", subscriptionID)
        return nil
    }
    
    // Update subscription history
    _, err = db.Exec(`
        UPDATE subscription_history 
        SET status = 'cancelled', ended_at = CURRENT_TIMESTAMP
        WHERE subscription_id = $1
    `, subscriptionID)
    
    logger.Printf("Subscription %s marked as cancelled", subscriptionID)
    return nil
}

// Update member status when subscription is suspended
func (db *Database) handleSubscriptionSuspended(event WebhookEvent) error {
    subscriptionID := event.Resource.ID
    
    result, err := db.Exec(`
        UPDATE members 
        SET subscription_status = 'suspended', 
            updated_at = CURRENT_TIMESTAMP
        WHERE current_subscription_id = $1
    `, subscriptionID)
    
    if err != nil {
        return fmt.Errorf("failed to update suspended subscription: %w", err)
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        logger.Printf("Warning: Subscription %s not found in database", subscriptionID)
        return nil
    }
    
    // Update subscription history
    _, err = db.Exec(`
        UPDATE subscription_history 
        SET status = 'suspended'
        WHERE subscription_id = $1
    `, subscriptionID)
    
    logger.Printf("Subscription %s marked as suspended", subscriptionID)
    return nil
}

// Update member status when subscription is reactivated
func (db *Database) handleSubscriptionReactivated(event WebhookEvent) error {
    subscriptionID := event.Resource.ID
    
    result, err := db.Exec(`
        UPDATE members 
        SET subscription_status = 'active', 
            updated_at = CURRENT_TIMESTAMP
        WHERE current_subscription_id = $1
    `, subscriptionID)
    
    if err != nil {
        return fmt.Errorf("failed to reactivate subscription: %w", err)
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        logger.Printf("Warning: Subscription %s not found in database", subscriptionID)
        return nil
    }
    
    // Update subscription history
    _, err = db.Exec(`
        UPDATE subscription_history 
        SET status = 'active', ended_at = NULL
        WHERE subscription_id = $1
    `, subscriptionID)
    
    logger.Printf("Subscription %s reactivated", subscriptionID)
    return nil
}

// Record payment and update payer_id if missing
func (db *Database) handlePaymentCompleted(event WebhookEvent) error {
    // Get payer information from various possible locations in the webhook
    var payerID, email string
    
    if event.Resource.Payer.PayerID != "" {
        payerID = event.Resource.Payer.PayerID
        email = event.Resource.Payer.EmailAddress
    } else if event.Resource.Payer.PayerInfo.PayerID != "" {
        payerID = event.Resource.Payer.PayerInfo.PayerID
        email = event.Resource.Payer.PayerInfo.Email
    }
    
    subscriptionID := event.Resource.BillingAgreementID
    if subscriptionID == "" {
        // Try to find subscription by payer email
        if email != "" {
            err := db.QueryRow(`
                SELECT current_subscription_id FROM members 
                WHERE paypal_email = $1
            `, strings.ToLower(email)).Scan(&subscriptionID)
            
            if err != nil {
                logger.Printf("Warning: Could not find subscription for payment from %s", email)
            }
        }
    }
    
    // Update member with payer_id if we don't have it
    if payerID != "" && subscriptionID != "" {
        _, err := db.Exec(`
            UPDATE members 
            SET paypal_payer_id = COALESCE(paypal_payer_id, $1),
                updated_at = CURRENT_TIMESTAMP
            WHERE current_subscription_id = $2 AND 
                  (paypal_payer_id IS NULL OR paypal_payer_id = '')
        `, payerID, subscriptionID)
        
        if err != nil {
            logger.Printf("Warning: Failed to update payer_id: %v", err)
        }
    }
    
    // Get member_id
    var memberID int
    err := db.QueryRow(`
        SELECT id FROM members 
        WHERE current_subscription_id = $1 OR paypal_payer_id = $2
        LIMIT 1
    `, subscriptionID, payerID).Scan(&memberID)
    
    if err != nil {
        logger.Printf("Warning: Could not find member for payment (subscription: %s, payer: %s)", 
            subscriptionID, payerID)
        return nil
    }
    
    // Record the payment
    _, err = db.Exec(`
        INSERT INTO payments (
            member_id,
            subscription_id,
            transaction_id,
            amount,
            currency,
            status
        ) VALUES ($1, $2, $3, $4, $5, 'completed')
        ON CONFLICT (transaction_id) DO NOTHING
    `, memberID, subscriptionID, event.Resource.ID, 
       event.Resource.Amount.Value, event.Resource.Amount.CurrencyCode)
    
    if err != nil {
        return fmt.Errorf("failed to record payment: %w", err)
    }
    
    logger.Printf("Payment recorded: %s %s for member_id %d", 
        event.Resource.Amount.Value, event.Resource.Amount.CurrencyCode, memberID)
    return nil
}

// Health check handler
func healthHandler(w http.ResponseWriter, r *http.Request) {
    // Check database connection
    dbStatus := "ok"
    if err := db.Ping(); err != nil {
        dbStatus = "error: " + err.Error()
    }
    
    response := map[string]interface{}{
        "status":    "ok",
        "timestamp": time.Now().Format(time.RFC3339),
        "database":  dbStatus,
    }
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(response)
}

// Stats handler - useful for monitoring
func statsHandler(w http.ResponseWriter, r *http.Request) {
    var stats struct {
        ActiveMembers   int     `json:"active_members"`
        TotalMembers    int     `json:"total_members"`
        MonthlyRevenue  float64 `json:"monthly_revenue"`
        SuspendedCount  int     `json:"suspended_count"`
    }
    
    // Get active members count
    db.QueryRow(`
        SELECT COUNT(*) FROM members 
        WHERE subscription_status = 'active'
    `).Scan(&stats.ActiveMembers)
    
    // Get total members
    db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&stats.TotalMembers)
    
    // Get monthly revenue
    db.QueryRow(`
        SELECT COALESCE(SUM(monthly_amount), 0) FROM members 
        WHERE subscription_status = 'active'
    `).Scan(&stats.MonthlyRevenue)
    
    // Get suspended count
    db.QueryRow(`
        SELECT COUNT(*) FROM members 
        WHERE subscription_status = 'suspended'
    `).Scan(&stats.SuspendedCount)
    
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(stats)
}

// PayPal webhook handler
func webhookHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
        return
    }
    
    // Read the raw body
    body, err := io.ReadAll(r.Body)
    if err != nil {
        logger.Printf("Error reading body: %v", err)
        http.Error(w, "Bad request", http.StatusBadRequest)
        return
    }
    defer r.Body.Close()
    
    // Parse the webhook event
    var event WebhookEvent
    if err := json.Unmarshal(body, &event); err != nil {
        logger.Printf("Error parsing JSON: %v", err)
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    // Log the incoming webhook
    logger.Printf("Received webhook: %s (ID: %s)", event.EventType, event.ID)
    
    // Get PayPal headers for verification
    headers := map[string]string{
        "paypal-auth-algo":        r.Header.Get("Paypal-Auth-Algo"),
        "paypal-cert-url":         r.Header.Get("Paypal-Cert-Url"),
        "paypal-transmission-id":  r.Header.Get("Paypal-Transmission-Id"),
        "paypal-transmission-sig": r.Header.Get("Paypal-Transmission-Sig"),
        "paypal-transmission-time": r.Header.Get("Paypal-Transmission-Time"),
    }
    
    // Verify webhook signature (if webhook ID is configured)
    if config.PayPalWebhookID != "" {
        if valid := verifyWebhookSignature(headers, body); !valid {
            logger.Println("Warning: Invalid webhook signature")
            // In production, you should return 401
            // http.Error(w, "Unauthorized", http.StatusUnauthorized)
            // return
        }
    }
    
    // Process the webhook event
    if err := processWebhookEvent(event); err != nil {
        logger.Printf("Error processing webhook: %v", err)
        // Still return 200 to prevent PayPal from retrying
    }
    
    // Always return 200 OK to PayPal
    w.WriteHeader(http.StatusOK)
    fmt.Fprint(w, "OK")
}

// Process different webhook events
func processWebhookEvent(event WebhookEvent) error {
    switch event.EventType {
    case "BILLING.SUBSCRIPTION.ACTIVATED":
        return db.handleSubscriptionActivated(event)
        
    case "BILLING.SUBSCRIPTION.CANCELLED":
        return db.handleSubscriptionCancelled(event)
        
    case "BILLING.SUBSCRIPTION.SUSPENDED":
        return db.handleSubscriptionSuspended(event)
        
    case "BILLING.SUBSCRIPTION.RE-ACTIVATED":
        return db.handleSubscriptionReactivated(event)
        
    case "PAYMENT.SALE.COMPLETED":
        return db.handlePaymentCompleted(event)
        
    default:
        logger.Printf("Unhandled event type: %s", event.EventType)
    }
    
    return nil
}

// Verify webhook signature with PayPal
func verifyWebhookSignature(headers map[string]string, body []byte) bool {
    // Get access token
    token, err := getPayPalAccessToken()
    if err != nil {
        logger.Printf("Error getting access token: %v", err)
        return false
    }
    
    // Prepare verification request
    verificationData := map[string]interface{}{
        "auth_algo":         headers["paypal-auth-algo"],
        "cert_url":          headers["paypal-cert-url"],
        "transmission_id":   headers["paypal-transmission-id"],
        "transmission_sig":  headers["paypal-transmission-sig"],
        "transmission_time": headers["paypal-transmission-time"],
        "webhook_id":        config.PayPalWebhookID,
        "webhook_event":     json.RawMessage(body),
    }
    
    jsonData, err := json.Marshal(verificationData)
    if err != nil {
        logger.Printf("Error marshaling verification data: %v", err)
        return false
    }
    
    // Send verification request
    req, err := http.NewRequest("POST", 
        config.PayPalBaseURL+"/v1/notifications/verify-webhook-signature",
        bytes.NewBuffer(jsonData))
    if err != nil {
        logger.Printf("Error creating verification request: %v", err)
        return false
    }
    
    req.Header.Set("Authorization", "Bearer "+token)
    req.Header.Set("Content-Type", "application/json")
    
    client := &http.Client{Timeout: 10 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        logger.Printf("Error sending verification request: %v", err)
        return false
    }
    defer resp.Body.Close()
    
    var result struct {
        VerificationStatus string `json:"verification_status"`
    }
    
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        logger.Printf("Error decoding verification response: %v", err)
        return false
    }
    
    return result.VerificationStatus == "SUCCESS"
}

// Get PayPal access token
func getPayPalAccessToken() (string, error) {
    data := "grant_type=client_credentials"
    req, err := http.NewRequest("POST",
        config.PayPalBaseURL+"/v1/oauth2/token",
        bytes.NewBufferString(data))
    if err != nil {
        return "", err
    }
    
    req.SetBasicAuth(config.PayPalClientID, config.PayPalClientSecret)
    req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
    
    client := &http.Client{Timeout: 10 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()
    
    var result struct {
        AccessToken string `json:"access_token"`
    }
    
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return "", err
    }
    
    return result.AccessToken, nil
}

// Middleware for logging requests
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
    
    // Create tables if they don't exist
    if err := db.createTables(); err != nil {
        logger.Fatalf("Failed to create tables: %v", err)
    }
    
    // Set up routes
    http.HandleFunc("/health", loggingMiddleware(healthHandler))
    http.HandleFunc("/stats", loggingMiddleware(statsHandler))
    http.HandleFunc("/api/paypal/webhook", loggingMiddleware(webhookHandler))
    
    // Start server
    addr := "127.0.0.1:" + config.Port
    logger.Printf("Starting webhook server on %s", addr)
    logger.Printf("Database connected successfully")
    logger.Printf("PayPal Base URL: %s", config.PayPalBaseURL)
    
    if err := http.ListenAndServe(addr, nil); err != nil {
        logger.Fatalf("Server failed to start: %v", err)
    }
}
