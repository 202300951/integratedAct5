const fs = require('node:fs');
const path = require('node:path');
const { handleScmWebhook } = require('./c.js');

const SCM_FILE = path.join(__dirname, 'a.json');
const PM_FILE = path.join(__dirname, 'b.json');

async function processScmUpdate() {
  try {
    const rawContent = fs.readFileSync(SCM_FILE, 'utf8');
    const scmPayload = JSON.parse(rawContent);

    console.log(`\n==================================================`);
    console.log(`[Node A - SCM] Ingested a.json: Event ID = ${scmPayload.event_id}`);
    console.log(`==================================================`);

    // Execute pipeline: a.json -> c.js -> b.js -> b.json
    const result = await handleScmWebhook(scmPayload);

    // Suppress output if duplicate or simulated failure
    if (result && result.duplicate) {
      console.log("\n>>> IDEMPOTENCY KEY BLOCKED <<<");
      console.log(`- Event ID '${scmPayload.event_id}' has already been processed.`);
      console.log("- Duplicate suppressed. Output for a.json and b.json withheld.\n");
    } else if (scmPayload.simulateFailure) {
      console.log("\n>>> NETWORK FAILURE SIMULATED <<<");
      console.log("- Connection dropped across all 3 retry attempts.");
      console.log("- Message diverted to Dead Letter Queue (DLQ).");
      console.log("- Saga compensatory rollback executed.");
      console.log("- Downstream delivery failed. Output for a.json and b.json withheld.\n");
    } else {
      console.log("\n>>> PIPELINE SYNCHRONIZATION SUCCESSFUL <<<");
      console.log("- b.json successfully updated:");
      console.log("\n==================== a.json ====================");
      console.log(fs.readFileSync(SCM_FILE, 'utf8'));
      console.log("\n==================== b.json ====================");
      console.log(fs.readFileSync(PM_FILE, 'utf8'));
      console.log("================================================\n");
    }

    return result;
  } catch (err) {
    console.error(`[Node A - SCM] Error reading or processing a.json:`, err.message);
    return { success: false, error: err.message };
  }
}

// Standalone execution: run on startup and watch a.json for direct edits
if (require.main === module) {
  let debounceTimer = null;
  fs.watch(SCM_FILE, (eventType) => {
    if (eventType === 'change') {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        processScmUpdate();
      }, 150);
    }
  });

  console.log(`[Node A - SCM] Watching a.json for live updates...`);
  processScmUpdate();
}

module.exports = { processScmUpdate };