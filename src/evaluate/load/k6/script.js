/**
 * k6 scenario for endure RQ1 scalability sweep.
 *
 * Uses constant-arrival-rate (open model): fires RATE job submissions per second
 * regardless of how long each job takes. No user-count tuning needed — k6
 * auto-allocates VUs to absorb the blocking poll loop.
 *
 * Environment variables (all optional):
 *   API_URL        base URL of the endure API  (default: http://localhost:8000)
 *   RATE           job submissions per second   (default: 5)
 *   DURATION       measurement window in seconds (default: 30)
 *   WARMUP         seconds before measurement starts (default: 5)
 *   STAGE_DURATION seconds per job stage        (default: 0.4)
 *   NUM_STAGES     stages per job               (default: 5)
 *   TENANT_NAME    tenant to use / create       (default: k6-tenant)
 *
 * Run standalone:
 *   k6 run --env RATE=5 --env DURATION=30 \
 *          --summary-export results.json \
 *          src/evaluate/load/k6/script.js
 */

import http from "k6/http";
import { sleep } from "k6";
import { check } from "k6";

const API_URL     = (__ENV.API_URL     || "http://localhost:8000").replace(/\/$/, "");
// constant-arrival-rate requires integer rate; express fractional rates by
// setting timeUnit="10s" and multiplying: e.g. 1.3/s → rate=13, timeUnit="10s".
const RATE_FLOAT  = parseFloat(__ENV.RATE       || "5");
const RATE        = Math.round(RATE_FLOAT * 10);   // iterations per 10 seconds
const DURATION    = parseInt(__ENV.DURATION    || "30");
const WARMUP      = parseInt(__ENV.WARMUP      || "5");
const STAGE_DUR   = parseFloat(__ENV.STAGE_DURATION || "0.4");
const NUM_STAGES  = parseInt(__ENV.NUM_STAGES  || "5");
const TENANT_NAME = __ENV.TENANT_NAME || "k6-tenant";

const TERMINAL = new Set(["COMPLETED", "FAILED", "DEAD_LETTER", "CANCELLED", "TIMED_OUT"]);
const HEADERS  = { "Content-Type": "application/json" };

export const options = {
  scenarios: {
    jobs: {
      executor:        "constant-arrival-rate",
      rate:            RATE,           // iterations per timeUnit (integer)
      timeUnit:        "10s",          // → RATE_FLOAT iterations/second
      duration:        `${DURATION}s`,
      preAllocatedVUs: Math.ceil(Math.max(40, RATE_FLOAT * 15)),
      maxVUs:          Math.ceil(Math.max(200, RATE_FLOAT * 30)),
      startTime:       `${WARMUP}s`,  // discard warm-up window from stats
      gracefulStop:    "5s",          // kill stale pollers quickly after scenario ends
    },
  },
  // No thresholds — this is a measurement run, not a pass/fail gate.
  summaryTrendStats: ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"],
};

// setup() runs once before any VU starts. Creates the tenant and returns its ID.
export function setup() {
  const body = JSON.stringify({
    name: TENANT_NAME,
    max_concurrent_jobs: 1000,
    max_workers: 100,
  });

  let tenantId;
  const create = http.post(`${API_URL}/api/v1/admin/tenants`, body, { headers: HEADERS });

  if (create.status === 201) {
    tenantId = create.json("id");
  } else if (create.status === 409) {
    // Tenant already exists from a previous run — look it up by name.
    const list = http.get(`${API_URL}/api/v1/admin/tenants`, { headers: HEADERS });
    const found = list.json().find((t) => t.name === TENANT_NAME);
    if (!found) throw new Error(`409 but tenant '${TENANT_NAME}' not in list`);
    tenantId = found.id;
  } else {
    throw new Error(`Failed to create tenant: ${create.status} ${create.body}`);
  }

  console.log(`Tenant ready: ${tenantId}`);
  return { tenantId };
}

// default() is the VU function — called at the arrival-rate cadence.
export default function (data) {
  const { tenantId } = data;

  // Submit job
  const submitRes = http.post(
    `${API_URL}/api/v1/jobs`,
    JSON.stringify({
      name:             `k6-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      tenant_id:        tenantId,
      job_type:         "src.evaluate.jobs:SyntheticJob",
      payload:          { stage_duration: STAGE_DUR, stages: NUM_STAGES },
      max_retries:      0,
      timeout_seconds:  300,
    }),
    { headers: HEADERS, tags: { name: "submit" } },
  );

  if (!check(submitRes, { "submit 201": (r) => r.status === 201 })) {
    return;
  }

  const jobId = submitRes.json("id");

  // Poll until terminal state, but abandon after DURATION seconds so stalled
  // iterations (jobs that never dequeued due to overload) don't pile up as pollers.
  const deadline = Date.now() + DURATION * 1000;
  while (Date.now() < deadline) {
    sleep(0.5);
    const pollRes = http.get(
      `${API_URL}/api/v1/jobs/${jobId}`,
      { tags: { name: "poll" } },
    );
    if (!pollRes || pollRes.status !== 200) continue;
    const state = pollRes.json("state");
    if (TERMINAL.has(state)) break;
  }
}
