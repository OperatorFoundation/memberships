-- Rename subscription_status to status
ALTER TABLE members RENAME COLUMN subscription_status TO status;

-- Combine given_name and surname into name
ALTER TABLE members ADD COLUMN name VARCHAR(255);
UPDATE members SET name = CONCAT(COALESCE(given_name, ''), ' ', COALESCE(surname, ''));
ALTER TABLE members DROP COLUMN given_name;
ALTER TABLE members DROP COLUMN surname;

-- Drop columns we don't need
ALTER TABLE members DROP COLUMN IF EXISTS referred_by;

-- Add missing columns
ALTER TABLE members ADD COLUMN IF NOT EXISTS first_seen DATE DEFAULT CURRENT_DATE;
ALTER TABLE members ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Rename updated_at to last_updated if it exists
ALTER TABLE members DROP COLUMN IF EXISTS created_at;
ALTER TABLE members DROP COLUMN IF EXISTS updated_at;
