const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');

const PORT = 5000;
const PM_FILE = path.join(__dirname, 'b.json');
const processedEvents = new Set();

let pmState = {
  projectId: "prj-01",
  taskId: "shp-01",
  taskStatus: "Active",
  scheduleExtensionDays: 0,
  totalDelayedUnits: 0,
  impactNotes: "No active delays recorded."
};

const server = http.createServer((req, res) => {
  if (req.method === 'POST' && req.url === '/api/v1/pm/tasks') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const payload = JSON.parse(body);

        // 1. Validate Idempotency Key
        if (!payload.idempotencyKey) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ error: "Missing idempotencyKey" }));
        }

        if (processedEvents.has(payload.idempotencyKey)) {
          console.log(`[Node B - PM] 409 Conflict: Suppressing duplicate key '${payload.idempotencyKey}'`);
          res.writeHead(409, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ status: "Conflict", duplicate: true }));
        }

        // 2. Validate Enum
        if (!["Active", "On_Hold", "Completed", "Cancelled"].includes(payload.taskStatus)) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          return res.end(JSON.stringify({ error: "Invalid taskStatus enum" }));
        }

        // 3. Process & Commit Update
        processedEvents.add(payload.idempotencyKey);
        pmState = { ...payload };

        // Write directly to b.json
        fs.writeFileSync(PM_FILE, JSON.stringify(pmState, null, 2), 'utf8');

        console.log(`\n==================== b.json ====================`);
        console.log(fs.readFileSync(PM_FILE, 'utf8'));
        console.log(`================================================\n`);

        res.writeHead(200, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ status: "Success", record: pmState }));
      } catch (err) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        return res.end(JSON.stringify({ error: "Invalid JSON format" }));
      }
    });
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: "Route not found" }));
  }
});

server.listen(PORT, () => {
  console.log(`=======================================================`);
  console.log(`[Node B - PM Server] REST API running on port ${PORT}`);
  console.log(`Endpoint: POST http://localhost:${PORT}/api/v1/pm/tasks`);
  console.log(`=======================================================`);
});