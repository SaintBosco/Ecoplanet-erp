const { Worker, Queue } = require('bullmq');
const IORedis = require('ioredis');

// Connect to Redis instance
const connection = new IORedis(process.env.REDIS_URL || 'redis://localhost:6379');

// Export Queue so the controller can add jobs to it
const accountingQueue = new Queue('accounting', { connection });

// Define the worker processor
const worker = new Worker('accounting', async (job) => {
  console.log(`Processing job ${job.id} of type ${job.name}`);
  
  if (job.name === 'generate-recurring-invoices') {
    // In a real system, you'd fetch recurring templates that are due today
    // and use InvoiceService.createInvoice()
    console.log('Generating recurring invoices...');
    
    // Simulate work
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    return { success: true, generated: 5 };
  }
  
  if (job.name === 'automated-bank-reconciliation') {
    console.log('Reconciling bank feeds...');
    // Fetch from Plaid/Bank feed API, match with un-reconciled AR/AP
    
    // Simulate work
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    return { success: true, matched: 12 };
  }
}, { connection });

worker.on('completed', (job, returnvalue) => {
  console.log(`Job ${job.id} completed with result:`, returnvalue);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed with error:`, err);
});

module.exports = { accountingQueue, worker };
