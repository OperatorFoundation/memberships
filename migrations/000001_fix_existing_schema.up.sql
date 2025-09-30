-- Handle existing tables gracefully
DO $$ 
BEGIN
    -- Check if old schema exists and migrate data
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name = 'members' 
               AND column_name = 'paypal_email') THEN
        
        -- Rename old column
        ALTER TABLE members RENAME COLUMN paypal_email TO email;
        
        -- Add new columns if they don't exist
        ALTER TABLE members ADD COLUMN IF NOT EXISTS is_anonymous BOOLEAN DEFAULT false;
        
        -- Drop PayPal-specific columns if they exist
        ALTER TABLE members DROP COLUMN IF EXISTS paypal_payer_id;
        ALTER TABLE members DROP COLUMN IF EXISTS current_subscription_id;
        ALTER TABLE members DROP COLUMN IF EXISTS monthly_amount;
        
    ELSIF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                      WHERE table_name = 'members' 
                      AND column_name = 'email') THEN
        
        -- Create fresh schema
        CREATE TABLE members (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            is_anonymous BOOLEAN DEFAULT false,
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            first_seen DATE DEFAULT CURRENT_DATE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    END IF;
END $$;

-- Create other tables if they don't exist
CREATE TABLE IF NOT EXISTS status_history (
    id SERIAL PRIMARY KEY,
    member_id INTEGER REFERENCES members(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS webhook_logs (
    id SERIAL PRIMARY KEY,
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    email VARCHAR(255),
    status VARCHAR(20),
    payload JSONB
);
