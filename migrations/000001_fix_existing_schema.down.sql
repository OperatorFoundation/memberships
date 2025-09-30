-- Rollback migration
DROP TABLE IF EXISTS webhook_logs CASCADE;
DROP TABLE IF EXISTS status_history CASCADE;
DROP TABLE IF EXISTS members CASCADE;
