const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline/promises');
const { stdin: input, stdout: output } = require('node:process');
const { processScmUpdate } = require('./a.js');

const A_JSON_FILE = path.join(__dirname, 'a.json');
const B_JSON_FILE = path.join(__dirname, 'b.json');

let eventCounter = 1;

async function main() {
  const rl = readline.createInterface({ input, output });

  while (true) {
    console.log("\n==================================================");
    console.log("             SCM PAYLOAD INPUT CLI                ");
    console.log("==================================================");

    const defaultEventId = `evt-0${eventCounter}`;
    const event_id = (await rl.question(`Enter Event ID [default: ${defaultEventId}]: `)).trim() || defaultEventId;
    const project_id = (await rl.question("Enter Project ID [default: prj-01]: ")).trim() || "prj-01";
    const shipmentId = (await rl.question("Enter Shipment ID [default: shp-01]: ")).trim() || "shp-01";
    const carrier_code = (await rl.question("Enter Carrier Code [default: car-01]: ")).trim() || "car-01";
    const new_status = (await rl.question("Enter SCM Status (Delayed / In_Transit) [default: Delayed]: ")).trim() || "Delayed";

    // Only prompt for delay metrics if the consignment status is Delayed
    let delay_duration_days = "0";
    let units_delayed = "0";
    let delayReason = "N/A";

    if (new_status === "Delayed") {
      delay_duration_days = (await rl.question("Enter Delay Days as String [default: 3]: ")).trim() || "3";
      units_delayed = (await rl.question("Enter Units Delayed as String [default: 50]: ")).trim() || "50";
      delayReason = (await rl.question("Enter Delay Reason [default: Port congestion]: ")).trim() || "Port congestion";
    }

    const simulateDrop = (await rl.question("Simulate Network Failure? (yes/no) [default: no]: ")).trim().toLowerCase() === "yes";

    const payload = {
      event_id,
      event_type: "SHIPMENT_DELAYED",
      timestamp: new Date().toISOString(),
      producer: "SCM-NODE-A",
      simulateFailure: simulateDrop,
      data: {
        project_id,
        shipmentId,
        carrier_code,
        previous_status: "IN_TRANSIT",
        new_status,
        delay_duration_days,
        units_delayed,
        delayReason
      }
    };

    // 1. Write to a.json
    fs.writeFileSync(A_JSON_FILE, JSON.stringify(payload, null, 2), 'utf8');

    // 2. Trigger pipeline: a.json -> a.js -> c.js -> b.js -> b.json
    console.log("\n>>> Executing Pipeline: a.json -> a.js -> c.js -> b.js -> b.json <<<");
    const result = await processScmUpdate();

    // 3. Display Results
    if (result && result.duplicate) {
      console.log("\n>>> IDEMPOTENCY KEY BLOCKED <<<");
      console.log(`- Event ID '${event_id}' has already been processed.`);
      console.log("- Duplicate suppressed. Output for a.json and b.json withheld.\n");
    } else if (simulateDrop) {
      console.log("\n>>> NETWORK FAILURE SIMULATED <<<");
      console.log("- Connection dropped across all 3 retry attempts.");
      console.log("- Message diverted to Dead Letter Queue (DLQ).");
      console.log("- Saga compensatory rollback executed.");
      console.log("- Downstream delivery failed. Output for a.json and b.json withheld.\n");
    } else {
      console.log("\n>>> PIPELINE SYNCHRONIZATION SUCCESSFUL <<<");
      console.log("- b.json successfully updated:");
      console.log("\n==================== a.json ====================");
      console.log(fs.readFileSync(A_JSON_FILE, 'utf8'));
      console.log("\n==================== b.json ====================");
      console.log(fs.readFileSync(B_JSON_FILE, 'utf8'));
      console.log("================================================\n");
    }

    eventCounter++;

    const runAgain = (await rl.question("Process another event? (yes/no): ")).trim().toLowerCase();
    if (runAgain !== "yes") {
      console.log("Exiting integration pipeline.");
      rl.close();
      process.exit(0);
    }
  }
}

main();