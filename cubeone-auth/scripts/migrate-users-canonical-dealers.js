require("dotenv").config();

const fs = require("fs");
const path = require("path");

const AUTH_DATA_DIR = String(process.env.AUTH_DATA_DIR || path.join(__dirname, "..")).trim() || path.join(__dirname, "..");
const USERS_FILE = path.join(AUTH_DATA_DIR, "users.json");
const AUTH_DEALER_ID_ALIASES_RAW = String(process.env.AUTH_DEALER_ID_ALIASES || "").trim();
const AUTH_EMAIL_DEALER_MAP_RAW = String(process.env.AUTH_EMAIL_DEALER_MAP || "").trim();

function normalizeDealerToken(input) {
  let v = String(input || "").trim().toLowerCase();
  if ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'"))) {
    v = v.slice(1, -1).trim();
  }
  return v;
}

function normalizeEmail(input) {
  return String(input || "").trim().toLowerCase();
}

function parseMap(raw) {
  const map = new Map();
  const text = String(raw || "").trim();
  if (!text) return map;
  for (const token of text.split(",")) {
    const pair = token.trim();
    if (!pair) continue;
    const idx = pair.indexOf("=");
    if (idx <= 0 || idx >= pair.length - 1) continue;
    const key = normalizeDealerToken(pair.slice(0, idx));
    const value = normalizeDealerToken(pair.slice(idx + 1));
    if (key && value) map.set(key, value);
  }
  return map;
}

const AUTH_DEALER_ID_ALIASES = parseMap(AUTH_DEALER_ID_ALIASES_RAW);
const AUTH_EMAIL_DEALER_MAP = parseMap(AUTH_EMAIL_DEALER_MAP_RAW);

function canonicalDealerId(input) {
  const raw = normalizeDealerToken(input);
  if (!raw) return "";
  if (AUTH_DEALER_ID_ALIASES.has(raw)) {
    return normalizeDealerToken(AUTH_DEALER_ID_ALIASES.get(raw));
  }
  const dealerPrefixNumeric = raw.match(/^dealer_(\d+)$/);
  if (dealerPrefixNumeric) return dealerPrefixNumeric[1];
  return raw;
}

function resolveUserDealerId(email, dealerId) {
  const normalizedEmail = normalizeEmail(email);
  const mapped = normalizedEmail ? AUTH_EMAIL_DEALER_MAP.get(normalizedEmail) : "";
  const resolved = canonicalDealerId(mapped || dealerId);
  return resolved || "dealer_default";
}

function main() {
  if (!fs.existsSync(USERS_FILE)) {
    console.error(`[migrate-users] users file not found: ${USERS_FILE}`);
    process.exit(1);
  }
  const parsed = JSON.parse(fs.readFileSync(USERS_FILE, "utf8"));
  if (!Array.isArray(parsed)) {
    console.error("[migrate-users] users file is not a JSON array");
    process.exit(1);
  }
  let changed = 0;
  const report = [];
  for (const user of parsed) {
    if (!user || typeof user !== "object") continue;
    const oldEmail = String(user.email || "");
    const oldDealer = String(user.dealerId || "");
    const nextEmail = normalizeEmail(oldEmail);
    const nextDealer = resolveUserDealerId(nextEmail, oldDealer);
    if (nextEmail && oldEmail !== nextEmail) {
      user.email = nextEmail;
      changed += 1;
    }
    if (nextDealer && oldDealer !== nextDealer) {
      user.dealerId = nextDealer;
      changed += 1;
      report.push({ email: nextEmail || oldEmail, from: oldDealer || null, to: nextDealer });
    }
  }
  fs.writeFileSync(USERS_FILE, JSON.stringify(parsed, null, 2), "utf8");
  console.log(JSON.stringify({
    ok: true,
    usersFile: USERS_FILE,
    aliasCount: AUTH_DEALER_ID_ALIASES.size,
    emailMapCount: AUTH_EMAIL_DEALER_MAP.size,
    fieldUpdates: changed,
    dealerChanges: report,
  }, null, 2));
}

main();
