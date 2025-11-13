#!/usr/bin/env node

const { Command } = require('commander');
const { fork } = require('child_process');
const path = require('path');
const queue = require('../src/queue');
const db = require('../src/db');

const program = new Command();

program
  .name('queuectl')
  .description('A simple background job queue system')
  .version('1.0.0');

// --- ENQUEUE ---
program
  .command('enqueue [args...]') 
  .description('Add a job. e.g: queuectl enqueue echo hello')
  .action((args) => {
    if (!args || args.length === 0) {
      console.error("❌ Error: Missing command.");
      return;
    }
    const rawInput = args.join(' '); 
    let commandStr;
    try {
      if (rawInput.trim().startsWith('{')) {
        const data = JSON.parse(rawInput);
        if (!data.command) throw new Error("Field 'command' is required");
        commandStr = data.command;
      } else {
        commandStr = rawInput;
      }
      const job = queue.enqueue(commandStr);
      console.log(`✅ Job enqueued! ID: ${job.id}`);
      console.log(`   Command: "${commandStr}"`);
    } catch (err) {
      console.error(`❌ Error: ${err.message}`);
    }
  });

// --- WORKER ---
const workerCommand = program.command('worker').description('Manage workers');

workerCommand
  .command('start')
  .description('Start worker processes')
  .option('-c, --count <number>', 'Number of workers', '1')
  .action((options) => {
    const count = parseInt(options.count, 10);
    if (isNaN(count)) {
      console.error("❌ Error: Count must be a number.");
      return;
    }
    console.log(`🚀 Starting ${count} worker(s)... (Press Ctrl+C to stop)`);
    const workers = [];
    const workerPath = path.join(__dirname, '../src/worker.js');
    for (let i = 0; i < count; i++) {
      workers.push(fork(workerPath));
    }
    process.on('SIGINT', () => {
        console.log("\n🛑 Stopping workers...");
        workers.forEach(w => w.kill('SIGINT'));
        process.exit();
    });
  });

workerCommand
  .command('stop')
  .description('Gracefully stop workers (not implemented)')
  .action(() => {
    console.log('To stop workers, press Ctrl+C in the terminal where they are running.');
    console.log('Proper graceful shutdown requires a PID file or IPC, which is not yet implemented.');
  });

// --- STATUS ---
program
  .command('status')
  .description('Show summary of all job states')
  .action(() => {
    try {
      const rows = db.prepare("SELECT state, COUNT(*) as count FROM jobs GROUP BY state").all();
      if (rows.length === 0) {
        console.log("No jobs in the queue.");
        return;
      }
      console.log("📊 Job Status Summary:");
      console.table(rows);
    } catch (err) {
      console.error(`❌ Error fetching status: ${err.message}`);
    }
  });

// --- LIST ---
program
  .command('list')
  .option('--state <state>', 'Filter by state (pending, processing, completed, failed, dead)')
  .description('List all jobs')
  .action((options) => {
    let query = "SELECT id, command, state, attempts, next_run_at FROM jobs";
    let args = [];
    if (options.state) {
      query += " WHERE state = ?";
      args.push(options.state);
    }
    const jobs = db.prepare(query).all(...args);
    console.table(jobs);
  });

// --- DLQ (Dead Letter Queue) ---
const dlqCommand = program.command('dlq').description('Manage the Dead Letter Queue');

dlqCommand
  .command('list')
  .description('List all jobs in the DLQ')
  .action(() => {
    const jobs = db.prepare("SELECT id, command, last_error, attempts FROM jobs WHERE state = 'dead'").all();
    if (jobs.length === 0) {
      console.log("✅ DLQ is empty.");
      return;
    }
    console.table(jobs);
  });

dlqCommand
  .command('retry <id>')
  .description('Retry a specific dead job')
  .action((id) => {
    const res = db.prepare(`
      UPDATE jobs 
      SET state = 'pending', attempts = 0, next_run_at = CURRENT_TIMESTAMP 
      WHERE id = ? AND state = 'dead'
    `).run(id);
    
    if (res.changes > 0) console.log(`♻️ Job ${id} moved back to pending.`);
    else console.log(`❌ Job not found or not dead.`);
  });

// --- CONFIG ---
const configCommand = program.command('config').description('Manage configuration');

configCommand
  .command('set <key> <value>')
  .description('Set a configuration value (e.g., max_retries)')
  .action((key, value) => {
    try {
      db.prepare("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)").run(key, value);
      console.log(`✅ Config updated: ${key} = ${value}`);
    } catch (err) {
      console.error(`❌ Error setting config: ${err.message}`);
    }
  });

configCommand
  .command('get <key>')
  .description('Get a configuration value')
  .action((key) => {
    try {
      const row = db.prepare("SELECT value FROM config WHERE key = ?").get(key);
      if (row) {
        console.log(`${key} = ${row.value}`);
      } else {
        console.log(`Config key "${key}" not found.`);
      }
    } catch (err) {
      console.error(`❌ Error setting config: ${err.message}`);
    }
  });


program.parse(process.argv);