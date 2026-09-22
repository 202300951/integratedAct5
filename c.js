const { processProjectUpdate } = require('./b.js');

// 1
const idempotencyStore = new Set();
const deadLetterQueue = [];

let pmDatabase = {
  projectId: "prj-01",
  taskId: "shp-01",
  taskStatus: "Active",
  scheduleExtensionDays: 0,
  totalDelayedUnits: 0,
  impactNotes: "No active delays recorded."
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

// 4
async function sendToPMWithRetry(transformedData, forceFailure = false, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;
    console.log(`[Middleware Node C] Dispatching to PM Node B (Attempt ${attempts}/${maxRetries})...`);

    await new Promise((resolve) => setTimeout(resolve, 250));

    if (forceFailure) {
      console.log(`[Network Error] Connection dropped on attempt ${attempts}.`);

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
        return { success: false, routedToDLQ: true, dlqRecord };
      }
    } else {
      const ingested = processProjectUpdate(transformedData);
      if (ingested) {
        pmDatabase.projectId = transformedData.projectId;
        pmDatabase.taskId = transformedData.taskId;
        pmDatabase.taskStatus = transformedData.taskStatus;
        pmDatabase.scheduleExtensionDays = transformedData.scheduleExtensionDays;
        pmDatabase.totalDelayedUnits = transformedData.totalDelayedUnits;
        pmDatabase.impactNotes = transformedData.impactNotes;
        return { success: true };
      } else {
        return { success: false, duplicate: true };
      }
    }
  }
}

// 5
async function handleScmWebhook(rawScmEvent) {
  const eventId = rawScmEvent.event_id || (rawScmEvent.data && rawScmEvent.data.event_id);

  console.log(`[Middleware Node C] Ingesting event: "${eventId}"`);

  if (idempotencyStore.has(eventId)) {
    console.log(`[IDEMPOTENCY BLOCKED] Event ID '${eventId}' has already been processed. Suppressing duplicate execution.`);
    return { success: false, duplicate: true };
  }

  idempotencyStore.add(eventId);

  const transformedPayload = transformSCMtoPM(rawScmEvent);
  console.log("[Middleware Node C] Transformed Target Contract:\n", JSON.stringify(transformedPayload, null, 2));

  const shouldFail = rawScmEvent.simulateFailure === true;
  const dispatchResult = await sendToPMWithRetry(transformedPayload, shouldFail);

  return dispatchResult;
}

module.exports = { handleScmWebhook };