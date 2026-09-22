const http = require('node:http');

const PORT = 4000;
const PM_ENDPOINT = { host: 'localhost', port: 5000, path: '/api/v1/pm/tasks' };

// 1
const idempotencyStore = new Set();
const deadLetterQueue = [];

let pmDatabase = {
  scheduleExtensionDays: 0,
  totalDelayedUnits: 0
};

// 2
function transformSCMtoPM(scmEvent) {
  const data = scmEvent.data || scmEvent;
  const isDelayed = data.new_status === "Delayed";

  const parsedDelay = parseInt(data.delay_duration_days, 10) || 0;
  const parsedUnits = parseInt(data.units_delayed, 10) || 0;

  const currentDays = typeof pmDatabase.scheduleExtensionDays === 'number' ? pmDatabase.scheduleExtensionDays : 0;
  const currentUnits = typeof pmDatabase.totalDelayedUnits === 'number' ? pmDatabase.totalDelayedUnits : 0;

  const cumulativeDays = currentDays + parsedDelay;
  const cumulativeUnits = currentUnits + parsedUnits;

  return {
    idempotencyKey: scmEvent.event_id || data.event_id,
    projectId: data.project_id,
    taskId: data.shipmentId,
    taskStatus: isDelayed ? "On_Hold" : "Active",
    scheduleExtensionDays: isDelayed ? cumulativeDays : "Not_Delayed",
    totalDelayedUnits: isDelayed ? cumulativeUnits : "Not_Delayed",
    impactNotes: isDelayed ? `Impact: ${data.delayReason} [Carrier: ${data.carrier_code}]` : "Not_Delayed"
  };
}

// 3
function executeSagaCompensatoryAction(payload, errorReason) {
  console.log("\n>>> [SAGA COMPENSATION TRIGGERED]");
  console.log(`Downstream delivery failed: ${errorReason}`);
  console.log(`Compensating Action: Reverting tentative holds for Key: ${payload.idempotencyKey}`);
  console.log(`Compensating Action: Notifying SCM logistics to release hold on Task: ${payload.taskId}`);
  console.log(">>> [COMPENSATION COMPLETE]\n");
}

// 4: Resilient HTTP Forwarder with Retries
function postHttp(options, body) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ statusCode: res.statusCode, body: JSON.parse(data || '{}') }));
    });
    req.on('error', reject);
    req.write(JSON.stringify(body));
    req.end();
  });
}

async function sendToPMWithRetry(transformedData, forceFailure = false, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;
    console.log(`[Middleware Node C] Dispatching HTTP POST to Node B (Attempt ${attempts}/${maxRetries})...`);

    await new Promise(resolve => setTimeout(resolve, 300));

    try {
      if (forceFailure) {
        throw new Error("ECONNRESET: Simulated network connection drop");
      }

      const res = await postHttp({
        hostname: PM_ENDPOINT.host,
        port: PM_ENDPOINT.port,
        path: PM_ENDPOINT.path,
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      }, transformedData);

      if (res.statusCode === 200) {
        if (typeof transformedData.scheduleExtensionDays === 'number') {
          pmDatabase.scheduleExtensionDays = transformedData.scheduleExtensionDays;
          pmDatabase.totalDelayedUnits = transformedData.totalDelayedUnits;
        }
        return { success: true };
      } else if (res.statusCode === 409) {
        return { success: false, duplicate: true };
      } else {
        throw new Error(`Node B rejected with status: ${res.statusCode}`);
      }
    } catch (err) {
      console.log(`[Network Error] Attempt ${attempts} failed: ${err.message}`);

      if (attempts >= maxRetries) {
        console.log(`\n[Resiliency Engine] All ${maxRetries} retries exhausted.`);

        const dlqRecord = {
          timestamp: new Date().toISOString(),
          failedPayload: transformedData,
          attempts: attempts
        };
        deadLetterQueue.push(dlqRecord);
        console.log("[DLQ] Message diverted to Dead Letter Queue.");

        executeSagaCompensatoryAction(transformedData, "PM API Connection Dropped / Timeout");
        return { success: false, routedToDLQ: true };
      }
    }
  }
}

// 5: Middleware REST Webhook Server
const server = http.createServer(async (req, res) => {
  if (req.method === 'POST' && req.url === '/api/v1/events/scm') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const scmEvent = JSON.parse(body);
        const eventId = scmEvent.event_id || (scmEvent.data && scmEvent.data.event_id);

        console.log(`\n[Middleware Node C] Ingested Webhook Event: "${eventId}"`);

        // Idempotency Filter
        if (idempotencyStore.has(eventId)) {
          console.log(`[IDEMPOTENCY BLOCKED] Event ID '${eventId}' already processed.`);
          res.writeHead(409, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ status: "Conflict", duplicate: true }));
        }

        idempotencyStore.add(eventId);

        // Transformation
        const transformedPayload = transformSCMtoPM(scmEvent);
        console.log("[Middleware Node C] Transformed Target Contract:\n", JSON.stringify(transformedPayload, null, 2));

        // Asynchronous Resilient Dispatch
        const shouldFail = scmEvent.simulateFailure === true;
        const result = await sendToPMWithRetry(transformedPayload, shouldFail);

        if (result.success) {
          res.writeHead(200, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ status: "Success" }));
        } else if (result.duplicate) {
          res.writeHead(409, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ status: "Conflict", duplicate: true }));
        } else {
          res.writeHead(502, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ status: "Bad Gateway", error: "Delivery failed" }));
        }
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: err.message }));
      }
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: "Route not found" }));
  }
});

server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`[Node C - Middleware Server] REST API running on port ${PORT}`);
  console.log(`Endpoint: POST http://localhost:${PORT}/api/v1/events/scm`);
  console.log(`=======================================================`);
});