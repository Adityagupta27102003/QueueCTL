const db = require('./db');
const { v4: uuidv4 } = require('crypto');

const generateId = () => require('crypto').randomUUID();

// --- MODIFIED ---
// Now reads max_retries from the config table
const enqueue = (command) => {
  // 1. Get current config
  const config = db.prepare("SELECT value FROM config WHERE key = 'max_retries'").get();
  const maxRetries = config ? parseInt(config.value, 10) : 3; // Default 3

  // 2. Insert job with that config
  const stmt = db.prepare(`
    INSERT INTO jobs (id, command, max_retries) 
    VALUES (?, ?, ?)
    RETURNING *
  `);
  return stmt.get(generateId(), command, maxRetries);
};
// --- END MODIFICATION ---

const fetchNextJob = () => {
  const stmt = db.prepare(`
    UPDATE jobs 
    SET state = 'processing', updated_at = CURRENT_TIMESTAMP
    WHERE id = (
      SELECT id FROM jobs 
      WHERE state = 'pending' 
      AND datetime(next_run_at) <= datetime('now')
      ORDER BY created_at ASC 
      LIMIT 1
    )
    RETURNING *
  `);
  return stmt.get();
};

const completeJob = (id) => {
  db.prepare("UPDATE jobs SET state = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = ?").run(id);
};

const failJob = (id, error, attempts, maxRetries) => {
  const nextAttempts = attempts + 1;
  
  if (nextAttempts >= maxRetries) {
    db.prepare(`
      UPDATE jobs 
      SET state = 'dead', last_error = ?, attempts = ?, updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `).run(error, nextAttempts, id);
    return 'dead';
  } else {
    const delaySeconds = Math.pow(2, nextAttempts); 
    db.prepare(`
      UPDATE jobs 
      SET state = 'pending', 
          last_error = ?, 
          attempts = ?, 
          next_run_at = datetime('now', '+' || ? || ' seconds'),
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `).run(error, nextAttempts, delaySeconds, id);
    return 'retry';
  }
};

module.exports = { enqueue, fetchNextJob, completeJob, failJob };