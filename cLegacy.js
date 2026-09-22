const readline = require('readline/promises');
const { stdin: input, stdout: output } = require('process');

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
  const parsedDelay = parseInt(scmEvent.delay_duration_days, 10) || 0;
  const parsedUnits = parseInt(scmEvent.units_delayed, 10) || 0;

  const cumulativeDays = pmDatabase.scheduleExtensionDays + parsedDelay;
  const cumulativeUnits = pmDatabase.totalDelayedUnits + parsedUnits;

  let mappedStatus = "Active";
  if (scmEvent.new_status === "Delayed") {
    mappedStatus = "On_Hold";
  }

  return {
    idempotencyKey: scmEvent.event_id,
    projectId: scmEvent.project_id,
    taskId: scmEvent.shipmentId,
    taskStatus: mappedStatus,
    scheduleExtensionDays: cumulativeDays,
    totalDelayedUnits: cumulativeUnits,
    impactNotes: `Impact: ${scmEvent.delayReason} [Carrier: ${scmEvent.carrier_code}]`
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
async function sendToPMWithRetry(transformedData, forceFailure, maxRetries = 3) {
  let attempts = 0;

  while (attempts < maxRetries) {
    attempts++;
    console.log(`[Middleware] Dispatching to PM Node B (Attempt ${attempts}/${maxRetries})...`);

    await new Promise((resolve) => setTimeout(resolve, 500));

    if (forceFailure) {
      console.log(`[Network Error] Connection dropped on attempt ${attempts}.`);

      if (attempts >= maxRetries) {
        console.log(`\n[Resiliency Engine] All ${maxRetries} retries exhausted.`);

        deadLetterQueue.push({
          timestamp: new Date().toISOString(),
          failedPayload: transformedData,
          attempts: attempts
        });
        console.log("[DLQ] Message diverted to Dead Letter Queue.");

        executeSagaCompensatoryAction(transformedData, "PM API Connection Dropped / Timeout");
        return false;
      }
    } else {
      pmDatabase.projectId = transformedData.projectId;
      pmDatabase.taskId = transformedData.taskId;
      pmDatabase.taskStatus = transformedData.taskStatus;
      pmDatabase.scheduleExtensionDays = transformedData.scheduleExtensionDays;
      pmDatabase.totalDelayedUnits = transformedData.totalDelayedUnits;
      pmDatabase.impactNotes = transformedData.impactNotes;
      return true;
    }
  }
}

// 5
async function main() {
  const rl = readline.createInterface({ input, output });

  while (true) {
    console.log("==================================================");
    console.log("   ITEC 116: TASK 2 MIDDLEWARE INTEGRATION NODE   ");
    console.log("==================================================");
    console.log("Current PM Database State:", JSON.stringify(pmDatabase, null, 2));
    console.log(`Dead Letter Queue (DLQ) Count: ${deadLetterQueue.length}`);
    console.log("--------------------------------------------------");

    const event_id = (await rl.question("Enter Event ID [default: evt-01]: ")).trim() || "evt-01";
    const project_id = (await rl.question("Enter Project ID [default: prj-01]: ")).trim() || "prj-01";
    const shipmentId = (await rl.question("Enter Shipment ID [default: shp-01]: ")).trim() || "shp-01";
    const carrier_code = (await rl.question("Enter Carrier Code [default: car-01]: ")).trim() || "car-01";
    const new_status = (await rl.question("Enter SCM Status (Delayed / In_Transit) [default: Delayed]: ")).trim() || "Delayed";
    const delay_duration_days = (await rl.question("Enter Delay Days as String [default: 3]: ")).trim() || "3";
    const units_delayed = (await rl.question("Enter Units Delayed as String [default: 50]: ")).trim() || "50";
    const delayReason = (await rl.question("Enter Delay Reason [default: Port congestion]: ")).trim() || "Port congestion";
    const simulateDrop = (await rl.question("Simulate Network Failure? (yes/no) [default: no]: ")).trim().toLowerCase() === "yes";

    const rawScmEvent = {
      event_id,
      project_id,
      shipmentId,
      carrier_code,
      new_status,
      delay_duration_days,
      units_delayed,
      delayReason
    };

    console.log("\n1. [Ingestion] Ingesting SCM event...");

    if (idempotencyStore.has(rawScmEvent.event_id)) {
      console.log(`\n[IDEMPOTENCY BLOCKED] Event ID '${rawScmEvent.event_id}' has already been processed. Suppressing duplicate execution.\n`);
    } else {
      idempotencyStore.add(rawScmEvent.event_id);

      const transformedPayload = transformSCMtoPM(rawScmEvent);
      console.log("\n2. [Transformed Payload (Target PM Contract)]:");
      console.log(JSON.stringify(transformedPayload, null, 2));

      console.log("\n3. [Delivery] Forwarding to PM application endpoint...");
      const success = await sendToPMWithRetry(transformedPayload, simulateDrop);

      if (success) {
        console.log("\n[SUCCESS] PM System updated according to mapping matrix!");
        console.log("Updated PM Database State:", JSON.stringify(pmDatabase, null, 2));
      } else {
        console.log("\n[FAILURE] Delivery failed. Payload routed to DLQ; compensatory rollback completed.");
      }
    }

    const runAgain = (await rl.question("\nProcess another event? (yes/no): ")).trim().toLowerCase();
    if (runAgain !== "yes") {
      console.log("Exiting middleware.");
      rl.close();
      process.exit(0);
    }
    console.log("\n");
  }
}

main();
