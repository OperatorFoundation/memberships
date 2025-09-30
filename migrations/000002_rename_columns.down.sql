-- Revert changes
ALTER TABLE members RENAME COLUMN status TO subscription_status;
ALTER TABLE members ADD COLUMN given_name VARCHAR(100);
ALTER TABLE members ADD COLUMN surname VARCHAR(100);
ALTER TABLE members DROP COLUMN name;
ALTER TABLE members ADD COLUMN referred_by VARCHAR(255);
