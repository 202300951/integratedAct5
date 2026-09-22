const fs = require('node:fs');
const path = require('node:path');

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

function processProjectUpdate(data) {
  if (!data.idempotencyKey) {
    throw new Error("Missing idempotencyKey");
  }

  if (processedEvents.has(data.idempotencyKey)) {
    console.log(`[Node B - PM] Duplicate suppressed for key: ${data.idempotencyKey}`);
    return false;
  }

  if (!["Active", "On_Hold", "Completed", "Cancelled"].includes(data.taskStatus)) {
    throw new Error(`Invalid taskStatus: ${data.taskStatus}`);
  }

  processedEvents.add(data.idempotencyKey);
  pmState = { ...data };

  // Write directly to b.json
  fs.writeFileSync(PM_FILE, JSON.stringify(pmState, null, 2), 'utf8');
  console.log(`[Node B - PM] Synchronized and wrote to b.json.`);

  return true;
}

module.exports = { processProjectUpdate };