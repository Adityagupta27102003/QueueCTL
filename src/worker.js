const { exec } = require('child_process');
const queue = require('./queue');

const WORKER_ID = process.pid;

async function start() {
  console.log(`[Worker ${WORKER_ID}] Started.`);

  let running = true;

  // Graceful Shutdown
  process.on('SIGINT', () => {
    console.log(`\n[Worker ${WORKER_ID}] Shutting down...`);
    running = false;
  });

  while (running) {
    const job = queue.fetchNextJob();

    if (!job) {
      // No jobs? Sleep 1 second to avoid CPU spin
      await new Promise(res => setTimeout(res, 1000));
      continue;
    }

    console.log(`[Worker ${WORKER_ID}] Processing job ${job.id}: ${job.command}`);

    // Wrap exec in Promise to await it
    try {
      await new Promise((resolve, reject) => {
        exec(job.command, (error, stdout, stderr) => {
          if (error) {
            reject(stderr || error.message);
          } else {
            console.log(`[Job ${job.id}] Output: ${stdout.trim()}`);
            resolve();
          }
        });
      });

      queue.completeJob(job.id);
      console.log(`[Worker ${WORKER_ID}] Job ${job.id} COMPLETED.`);
      
    } catch (err) {
      const result = queue.failJob(job.id, err, job.attempts, job.max_retries);
      console.error(`[Worker ${WORKER_ID}] Job ${job.id} FAILED. Action: ${result.toUpperCase()}`);
    }
  }
}

// If run directly, start the worker
if (require.main === module) {
  start();
}

module.exports = { start };