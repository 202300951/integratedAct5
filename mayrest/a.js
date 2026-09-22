const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');

const SCM_FILE = path.join(__dirname, 'a.json');
const PM_FILE = path.join(__dirname, 'b.json');

// Helper to send HTTP POST to Node C (REST Middleware)
function sendWebhook(payload) {
  return new Promise((resolve) => {
    const data = JSON.stringify(payload);
    const req = http.request({
      hostname: 'localhost',
      port: 4000,
      path: '/api/v1/events/scm',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(data)
      }
    }, (res) => {
      let body = '';
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => {
        try {
          resolve({ statusCode: res.statusCode, body: JSON.parse(body || '{}') });
        } catch {
          resolve({ statusCode: res.statusCode, body: {} });
        }
      });
    });

    req.on('error', (err) => resolve({ statusCode: 500, error: err.message }));
    req.write(data);
    req.end();
  });
}

async function processScmUpdate(isCliCall = false) {
  try {
    const rawContent = fs.readFileSync(SCM_FILE, 'utf8');
    const scmPayload = JSON.parse(rawContent);

    console.log(`\n==================================================`);
    console.log(`[Node A - SCM] Ingested a.json: Dispatching to Node C REST Endpoint...`);
    console.log(`==================================================`);

    const response = await sendWebhook(scmPayload);

    const isDuplicate = response.statusCode === 409 || (response.body && response.body.duplicate);
    const isFailure = response.statusCode === 502 || scmPayload.simulateFailure;

    // If run standalone (node a.js), print output here
    if (!isCliCall) {
      if (isDuplicate) {
        console.log("\n>>> IDEMPOTENCY KEY BLOCKED <<<");
        console.log(`- Event ID '${scmPayload.event_id}' has already been processed.`);
        console.log("- Duplicate suppressed. Output for a.json and b.json withheld.\n");
      } else if (isFailure) {
        console.log("\n>>> NETWORK FAILURE SIMULATED <<<");
        console.log("- Connection dropped across all 3 retry attempts.");
        console.log("- Message diverted to Dead Letter Queue (DLQ).");
        console.log("- Saga compensatory rollback executed.");
        console.log("- Downstream delivery failed. Output for a.json and b.json withheld.\n");
      } else if (response.statusCode === 200) {
        console.log("\n>>> PIPELINE SYNCHRONIZATION SUCCESSFUL <<<");
        console.log("- b.json successfully updated over REST API:");
        console.log("\n==================== a.json ====================");
        console.log(fs.readFileSync(SCM_FILE, 'utf8'));
        console.log("\n==================== b.json ====================");
        console.log(fs.readFileSync(PM_FILE, 'utf8'));
        console.log("================================================\n");
      }
    }

    return {
      statusCode: response.statusCode,
      duplicate: isDuplicate,
      failure: isFailure,
      success: response.statusCode === 200
    };
  } catch (err) {
    console.error(`[Node A - SCM] Error:`, err.message);
    return { error: true, message: err.message };
  }
}

// Standalone mode: Only watch file if running `node a.js` directly
if (require.main === module) {
  let debounceTimer = null;
  fs.watch(SCM_FILE, (eventType) => {
    if (eventType === 'change') {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        processScmUpdate(false);
      }, 150);
    }
  });

  console.log(`[Node A - SCM] Watching a.json for live updates...`);
  processScmUpdate(false);
}

module.exports = { processScmUpdate };