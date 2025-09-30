package main

import (
    "database/sql"
    "time"
)

// Config holds application configuration
type Config struct {
    DatabaseURL   string
    Port          string
    WebhookSecret string
}

// MemberWebhook represents the incoming webhook payload from Zapier
type MemberWebhook struct {
    Email     string `json:"email"`
    Name      string `json:"name"`
    Status    string `json:"status"`    // Zapier sends "Succeeded", "Failed", etc.
    Anonymous string `json:"anonymous"` // Zapier sends "True", "False" as strings
}

// Member represents a member in the database
type Member struct {
    ID          int
    Email       string
    Name        sql.NullString
    IsAnonymous bool
    Status      string
    FirstSeen   time.Time
    LastUpdated time.Time
}

// Stats represents membership statistics
type Stats struct {
    TotalMembers     int `json:"total_members"`
    ActiveMembers    int `json:"active_members"`
    CancelledMembers int `json:"cancelled_members"`
    AnonymousMembers int `json:"anonymous_members"`
}
