const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.join(process.cwd(), 'queue.db');
const db = new Database(dbPath);

db.pragma('journal_mode = WAL');

const initQuery = `
  CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    command TEXT NOT NULL,
    state TEXT DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    next_run_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_error TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  
  CREATE INDEX IF NOT EXISTS idx_poll ON jobs(state, next_run_at);

  -- NEW: Table for storing configuration
  CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT
  );

  -- Set default max_retries if not set
  INSERT OR IGNORE INTO config (key, value) VALUES ('max_retries', '3');
`;

db.exec(initQuery);

module.exports = db;