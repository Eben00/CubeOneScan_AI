/* eslint-disable no-console */
const BASE_URL = process.env.CONNECTOR_BASE_URL || "http://localhost:8080";
const API_KEY = process.env.CONNECTOR_API_KEY || process.env.API_KEY || "";

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

async function getJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch (_) {
    json = null;
  }
  return { res, json, text };
}

async function main() {
  console.log(`[smoke] checking ${BASE_URL}/healthz`);
  const health = await getJson(`${BASE_URL}/healthz`);
  assert(health.res.ok, `Health failed: HTTP ${health.res.status}`);
  assert(health.json?.ok === true, "Health response missing ok=true");

  if (!API_KEY) {
    console.log("[smoke] skipping command API check (CONNECTOR_API_KEY/API_KEY not set)");
    console.log("[smoke] PASS");
    return;
  }

  const correlationId = `smoke_${Date.now()}`;
  const body = {
    commandType: "CREATE_LEAD",
    correlationId,
    payload: {
      driverLicense: {
        firstName: "Smoke",
        lastName: "Tester",
      },
    },
  };

  console.log("[smoke] posting CREATE_LEAD");
  const createLead = await getJson(`${BASE_URL}/api/v1/commands`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${API_KEY}`,
    },
    body: JSON.stringify(body),
  });
  assert(createLead.res.status === 202 || createLead.res.status === 200, `CREATE_LEAD unexpected HTTP ${createLead.res.status}: ${createLead.text}`);

  console.log("[smoke] fetching command status");
  const status = await getJson(`${BASE_URL}/api/v1/commands/${correlationId}`, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
    },
  });
  assert(status.res.ok, `Status lookup failed: HTTP ${status.res.status}`);
  assert(status.json?.correlationId === correlationId, "Status lookup correlationId mismatch");

  console.log("[smoke] PASS");
}

main().catch((err) => {
  console.error(`[smoke] FAIL: ${err.message}`);
  process.exit(1);
});
