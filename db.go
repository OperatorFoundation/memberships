package main

import (
    "database/sql"
    "encoding/json"
    "fmt"
    "strings"
    "time"

    _ "github.com/lib/pq"
)

// Database wraps the SQL database connection
type Database struct {
    *sql.DB
}

// NewDatabase creates a new database connection
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
    
    return &Database{conn}, nil
}

// ProcessMember handles creating or updating a member from webhook data
func (db *Database) ProcessMember(email, name string, isAnonymous bool, status string) error {
    email = strings.ToLower(strings.TrimSpace(email))
    
    if email == "" {
        return fmt.Errorf("email is required")
    }
    
    // Don't store name for anonymous members
    if isAnonymous {
        name = ""
    }
    
    // Check if member exists
    var memberID int
    var currentStatus string
    err := db.QueryRow(`
        SELECT id, status FROM members WHERE email = $1
    `, email).Scan(&memberID, &currentStatus)
    
    if err == sql.ErrNoRows {
        // Create new member
        err = db.QueryRow(`
            INSERT INTO members (email, name, is_anonymous, status, first_seen, last_updated)
            VALUES ($1, $2, $3, $4, CURRENT_DATE, CURRENT_TIMESTAMP)
            RETURNING id
        `, email, name, isAnonymous, status).Scan(&memberID)
        
        if err != nil {
            return fmt.Errorf("failed to create member: %w", err)
        }
        
        logger.Printf("Created new member: %s (ID: %d, Status: %s)", email, memberID, status)
        
        // Record initial status in history
        _, _ = db.Exec(`
            INSERT INTO status_history (member_id, status)
            VALUES ($1, $2)
        `, memberID, status)
        
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
        `, isAnonymous, name, status, memberID)
        
        if err != nil {
            return fmt.Errorf("failed to update member: %w", err)
        }
        
        // Record status change if different
        if currentStatus != status {
            _, _ = db.Exec(`
                INSERT INTO status_history (member_id, status)
                VALUES ($1, $2)
            `, memberID, status)
            
            logger.Printf("Updated member %s (ID: %d): %s -> %s", 
                email, memberID, currentStatus, status)
        } else {
            logger.Printf("Member %s (ID: %d) status unchanged: %s", 
                email, memberID, status)
        }
    } else {
        return fmt.Errorf("database error: %w", err)
    }
    
    return nil
}

// LogWebhook stores the raw webhook data for debugging
func (db *Database) LogWebhook(email, status string, payload json.RawMessage) error {
    _, err := db.Exec(`
        INSERT INTO webhook_logs (email, status, payload)
        VALUES ($1, $2, $3)
    `, email, status, payload)
    return err
}

// GetStats returns membership statistics
func (db *Database) GetStats() (*Stats, error) {
    var stats Stats
    
    err := db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&stats.TotalMembers)
    if err != nil {
        return nil, err
    }
    
    err = db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'active'`).Scan(&stats.ActiveMembers)
    if err != nil {
        return nil, err
    }
    
    err = db.QueryRow(`SELECT COUNT(*) FROM members WHERE status = 'cancelled'`).Scan(&stats.CancelledMembers)
    if err != nil {
        return nil, err
    }
    
    err = db.QueryRow(`SELECT COUNT(*) FROM members WHERE is_anonymous = true`).Scan(&stats.AnonymousMembers)
    if err != nil {
        return nil, err
    }
    
    return &stats, nil
}

// GetMembers returns a list of members, optionally filtered by status
func (db *Database) GetMembers(statusFilter string, limit int) ([]map[string]interface{}, error) {
    query := `
        SELECT email, name, is_anonymous, status, first_seen, last_updated
        FROM members
    `
    args := []interface{}{}
    
    if statusFilter != "" {
        query += " WHERE status = $1"
        args = append(args, statusFilter)
    }
    
    query += fmt.Sprintf(" ORDER BY last_updated DESC LIMIT %d", limit)
    
    rows, err := db.Query(query, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var members []map[string]interface{}
    for rows.Next() {
        var email, name, status sql.NullString
        var isAnonymous sql.NullBool
        var firstSeen, lastUpdated sql.NullTime
        
        err := rows.Scan(&email, &name, &isAnonymous, &status, &firstSeen, &lastUpdated)
        if err != nil {
            continue
        }
        
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
    
    return members, nil
}

// GetAllMemberStatuses returns a map of email -> status for all members
func (db *Database) GetAllMemberStatuses() (map[string]string, error) {
    rows, err := db.Query(`SELECT email, status FROM members`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    members := make(map[string]string)
    for rows.Next() {
        var email, status string
        if err := rows.Scan(&email, &status); err != nil {
            continue
        }
        members[strings.ToLower(email)] = status
    }
    
    return members, nil
}

// UpdateMemberStatus updates just the status for a member
func (db *Database) UpdateMemberStatus(email, status string) error {
    email = strings.ToLower(strings.TrimSpace(email))
    
    result, err := db.Exec(`
        UPDATE members 
        SET status = $1, last_updated = CURRENT_TIMESTAMP
        WHERE email = $2
    `, status, email)
    
    if err != nil {
        return err
    }
    
    rows, _ := result.RowsAffected()
    if rows == 0 {
        return fmt.Errorf("member not found: %s", email)
    }
    
    // Record status change in history
    var memberID int
    db.QueryRow(`SELECT id FROM members WHERE email = $1`, email).Scan(&memberID)
    if memberID > 0 {
        db.Exec(`
            INSERT INTO status_history (member_id, status)
            VALUES ($1, $2)
        `, memberID, status)
    }
    
    return nil
}

// HealthCheck verifies database connectivity
func (db *Database) HealthCheck() error {
    return db.Ping()
}

// GetRecentMembers returns the most recently updated members
func (db *Database) GetRecentMembers(limit int) ([]map[string]interface{}, error) {
    query := `
        SELECT email, name, status, last_updated
        FROM members
        ORDER BY last_updated DESC
        LIMIT $1
    `
    
    rows, err := db.Query(query, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    
    var members []map[string]interface{}
    for rows.Next() {
        var email, name, status sql.NullString
        var lastUpdated sql.NullTime
        
        err := rows.Scan(&email, &name, &status, &lastUpdated)
        if err != nil {
            continue
        }
        
        member := map[string]interface{}{
            "email":        email.String,
            "status":       status.String,
            "last_updated": lastUpdated.Time.Format("2006-01-02 15:04:05"),
        }
        
        if name.Valid && name.String != "" {
            member["name"] = name.String
        }
        
        members = append(members, member)
    }
    
    return members, nil
}
