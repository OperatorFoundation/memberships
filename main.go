package main

import (
    "encoding/csv"
    "flag"
    "fmt"
    "log"
    "os"
    "strings"

    "github.com/joho/godotenv"
)

var logger *log.Logger

func main() {
    // Set up logger
    logger = log.New(os.Stdout, "[MEMBERSHIP] ", log.LstdFlags|log.Lshortfile)
    
    // Handle subcommands
    if len(os.Args) < 2 {
        // No subcommand - run webhook server
        runServer()
        return
    }
    
    switch os.Args[1] {
    case "server":
        runServer()
    case "clean":
        runClean()
    case "stats":
        runStats()
    case "help", "-h", "--help":
        printHelp()
    default:
        // If first arg doesn't match any subcommand, assume server mode
        runServer()
    }
}

func printHelp() {
    fmt.Println(`Membership Manager

Usage:
  memberships                    Run the webhook server (default)
  memberships server             Run the webhook server
  memberships clean <csv-file>   Sync database with GiveLively CSV export
  memberships stats              Display membership statistics
  memberships help               Show this help message

Environment variables:
  DATABASE_URL     PostgreSQL connection string (required)
  WEBHOOK_SECRET   Secret for authenticating webhooks (required for server)
  PORT            Port to listen on (default: 3000)`)
}

func runStats() {
    // Load .env file
    if err := godotenv.Load(); err != nil {
        logger.Println("No .env file found")
    }
    
    // Get database URL
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        logger.Fatal("DATABASE_URL environment variable is required")
    }
    
    // Connect to database
    db, err := NewDatabase(dbURL)
    if err != nil {
        logger.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
    
    // Get stats
    stats, err := db.GetStats()
    if err != nil {
        logger.Fatalf("Failed to get stats: %v", err)
    }
    
    // Display stats
    fmt.Println("\n=== Membership Statistics ===")
    fmt.Printf("Total Members:      %d\n", stats.TotalMembers)
    fmt.Printf("Active Members:     %d\n", stats.ActiveMembers)
    fmt.Printf("Cancelled Members:  %d\n", stats.CancelledMembers)
    fmt.Printf("Anonymous Members:  %d\n", stats.AnonymousMembers)
    
    // Calculate and display percentages if there are members
    if stats.TotalMembers > 0 {
        activePercent := float64(stats.ActiveMembers) * 100.0 / float64(stats.TotalMembers)
        cancelledPercent := float64(stats.CancelledMembers) * 100.0 / float64(stats.TotalMembers)
        anonymousPercent := float64(stats.AnonymousMembers) * 100.0 / float64(stats.TotalMembers)
        
        fmt.Println("\n=== Percentages ===")
        fmt.Printf("Active:    %.1f%%\n", activePercent)
        fmt.Printf("Cancelled: %.1f%%\n", cancelledPercent)
        fmt.Printf("Anonymous: %.1f%%\n", anonymousPercent)
    }
    
    // Get recent activity
    recentMembers, err := db.GetRecentMembers(5)
    if err == nil && len(recentMembers) > 0 {
        fmt.Println("\n=== Recent Members ===")
        for _, member := range recentMembers {
            email := member["email"]
            status := member["status"]
            updated := member["last_updated"]
            fmt.Printf("  %s (%s) - Updated: %v\n", email, status, updated)
        }
    }
    
    fmt.Println()
}

func runServer() {
    // Load .env file
    if err := godotenv.Load(); err != nil {
        logger.Println("No .env file found")
    }
    
    // Build configuration
    config := &Config{
        DatabaseURL:   os.Getenv("DATABASE_URL"),
        Port:          getEnvOrDefault("PORT", "3000"),
        WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
    }
    
    // Validate required configuration
    if config.DatabaseURL == "" {
        logger.Fatal("DATABASE_URL environment variable is required")
    }
    
    if config.WebhookSecret == "" {
        logger.Fatal("WEBHOOK_SECRET environment variable is required")
    }
    
    // Connect to database
    logger.Println("Connecting to database...")
    db, err := NewDatabase(config.DatabaseURL)
    if err != nil {
        logger.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
    logger.Println("Database connected successfully")
    
    // Start webhook server
    server := NewWebhookServer(db, config)
    logger.Printf("Starting server on port %s...", config.Port)
    
    if err := server.Start(); err != nil {
        logger.Fatalf("Server failed: %v", err)
    }
}

func runClean() {
    // Parse flags for clean subcommand
    cleanCmd := flag.NewFlagSet("clean", flag.ExitOnError)
    dryRun := cleanCmd.Bool("dry-run", false, "Show what would change without making changes")
    verbose := cleanCmd.Bool("verbose", false, "Show detailed output")
    
    // Need at least "memberships clean filename.csv"
    if len(os.Args) < 3 {
        fmt.Println("Error: clean command requires a CSV filename")
        fmt.Println("Usage: memberships clean <csv-file> [--dry-run] [--verbose]")
        os.Exit(1)
    }
    
    // Parse flags (everything after the filename)
    cleanCmd.Parse(os.Args[3:])
    
    csvFile := os.Args[2]
    
    // Load .env file
    if err := godotenv.Load(); err != nil {
        logger.Println("No .env file found")
    }
    
    // Get database URL
    dbURL := os.Getenv("DATABASE_URL")
    if dbURL == "" {
        logger.Fatal("DATABASE_URL environment variable is required")
    }
    
    // Connect to database
    logger.Println("Connecting to database...")
    db, err := NewDatabase(dbURL)
    if err != nil {
        logger.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()
    
    // Process the CSV file
    if err := cleanDatabase(db, csvFile, *dryRun, *verbose); err != nil {
        logger.Fatalf("Clean failed: %v", err)
    }
}

func cleanDatabase(db *Database, csvFile string, dryRun, verbose bool) error {
    logger.Printf("Processing CSV file: %s", csvFile)
    
    if dryRun {
        logger.Println("DRY RUN MODE - No changes will be made")
    }
    
    // Open CSV file
    file, err := os.Open(csvFile)
    if err != nil {
        return fmt.Errorf("failed to open CSV file: %w", err)
    }
    defer file.Close()
    
    // Parse CSV
    reader := csv.NewReader(file)
    
    // Read header row
    headers, err := reader.Read()
    if err != nil {
        return fmt.Errorf("failed to read CSV headers: %w", err)
    }
    
    // Find column indices we care about
    var (
        emailIdx     = -1
        frequencyIdx = -1
        statusIdx    = -1
    )
    
    for i, header := range headers {
        switch header {
        case "Email":
            emailIdx = i
        case "Frequency":
            frequencyIdx = i
        case "Payment Status":
            statusIdx = i
        }
    }
    
    if emailIdx == -1 {
        return fmt.Errorf("CSV missing required Email column")
    }
    
    // Track active recurring members from CSV
    activeMembers := make(map[string]bool)
    
    // Process each row
    rowCount := 0
    recurringCount := 0
    
    for {
        row, err := reader.Read()
        if err != nil {
            break // End of file
        }
        
        rowCount++
        
        // Skip if not enough columns
        if len(row) <= emailIdx {
            continue
        }
        
        email := strings.ToLower(strings.TrimSpace(row[emailIdx]))
        if email == "" {
            continue
        }
        
        // Check if this is a recurring donation
        frequency := ""
        if frequencyIdx >= 0 && frequencyIdx < len(row) {
            frequency = row[frequencyIdx]
        }
        
        // Only process recurring donations (Monthly, Quarterly, Annual, etc.)
        if frequency == "" || strings.ToLower(frequency) == "one-time" {
            if verbose {
                logger.Printf("Skipping one-time donation from %s", email)
            }
            continue
        }
        
        // Check payment status
        status := "active"
        if statusIdx >= 0 && statusIdx < len(row) {
            paymentStatus := strings.ToLower(row[statusIdx])
            if strings.Contains(paymentStatus, "succeed") {
                status = "active"
            } else if strings.Contains(paymentStatus, "fail") || strings.Contains(paymentStatus, "cancel") {
                status = "cancelled"
            }
        }
        
        // Only track active recurring members
        if status == "active" && frequency != "" {
            activeMembers[email] = true
            recurringCount++
            
            if verbose {
                logger.Printf("Found active recurring member: %s (%s)", email, frequency)
            }
        }
    }
    
    logger.Printf("Processed %d rows, found %d active recurring members", rowCount, recurringCount)
    
    // Get current members from database
    currentMembers, err := db.GetAllMemberStatuses()
    if err != nil {
        return fmt.Errorf("failed to get current members: %w", err)
    }
    
    logger.Printf("Database currently has %d members", len(currentMembers))
    
    // Find members to update
    toActivate := []string{}
    toDeactivate := []string{}
    
    for email, dbStatus := range currentMembers {
        if activeMembers[email] {
            // Member is in CSV as active
            if dbStatus != "active" {
                toActivate = append(toActivate, email)
            }
        } else {
            // Member is not in CSV (or not active)
            if dbStatus == "active" {
                toDeactivate = append(toDeactivate, email)
            }
        }
    }
    
    // Find new members to add (in CSV but not in database)
    toAdd := []string{}
    for email := range activeMembers {
        if _, exists := currentMembers[email]; !exists {
            toAdd = append(toAdd, email)
        }
    }
    
    // Report what will change
    logger.Printf("Changes to make:")
    logger.Printf("  - New members to add: %d", len(toAdd))
    logger.Printf("  - Members to reactivate: %d", len(toActivate))
    logger.Printf("  - Members to deactivate: %d", len(toDeactivate))
    
    if verbose {
        if len(toAdd) > 0 {
            logger.Printf("  New members: %v", toAdd)
        }
        if len(toActivate) > 0 {
            logger.Printf("  To activate: %v", toActivate)
        }
        if len(toDeactivate) > 0 {
            logger.Printf("  To deactivate: %v", toDeactivate)
        }
    }
    
    // Apply changes if not dry run
    if !dryRun {
        // Add new members
        for _, email := range toAdd {
            if err := db.ProcessMember(email, "", false, "active"); err != nil {
                logger.Printf("Error adding member %s: %v", email, err)
            } else if verbose {
                logger.Printf("Added member: %s", email)
            }
        }
        
        // Activate members
        for _, email := range toActivate {
            if err := db.UpdateMemberStatus(email, "active"); err != nil {
                logger.Printf("Error activating member %s: %v", email, err)
            } else if verbose {
                logger.Printf("Activated member: %s", email)
            }
        }
        
        // Deactivate members
        for _, email := range toDeactivate {
            if err := db.UpdateMemberStatus(email, "cancelled"); err != nil {
                logger.Printf("Error deactivating member %s: %v", email, err)
            } else if verbose {
                logger.Printf("Deactivated member: %s", email)
            }
        }
        
        logger.Println("Database sync complete!")
    } else {
        logger.Println("DRY RUN complete - no changes made")
    }
    
    return nil
}

func getEnvOrDefault(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}
