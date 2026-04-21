require("dotenv").config();

const express = require("express");
const cors = require("cors");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const fs = require("fs");
const path = require("path");
const { v4: uuidv4 } = require("uuid");

const app = express();
app.use(express.json());
app.use(
  cors({
    origin: "*", // tighten later if needed
  })
);

const PORT = process.env.PORT || 4000;
const JWT_SECRET = process.env.JWT_SECRET || "CHANGE_ME_JWT_SECRET";
const AUTH_DATA_DIR = String(process.env.AUTH_DATA_DIR || __dirname).trim() || __dirname;
const USERS_FILE = path.join(AUTH_DATA_DIR, "users.json");
const AUDIT_FILE = path.join(AUTH_DATA_DIR, "audit-log.json");
const SESSION_FILE = path.join(AUTH_DATA_DIR, "session-state.json");
function normalizeConfigToken(input) {
  let v = String(input || "").trim();
  if ((v.startsWith("\"") && v.endsWith("\"")) || (v.startsWith("'") && v.endsWith("'"))) {
    v = v.slice(1, -1).trim();
  }
  return v;
}

function normalizeConfigMap(raw) {
  const out = [];
  if (!raw) return "";
  if (typeof raw === "string") return normalizeConfigToken(raw);
  if (typeof raw === "object") {
    for (const [k, v] of Object.entries(raw)) {
      const key = normalizeConfigToken(k);
      const value = normalizeConfigToken(v);
      if (key && value) out.push(`${key}=${value}`);
    }
  }
  return out.join(",");
}

function loadTenantConfig() {
  const rawJson = String(process.env.TENANT_CONFIG_JSON || "").trim();
  const configFile = String(process.env.TENANT_CONFIG_FILE || "").trim();
  const runtimeFile = path.join(AUTH_DATA_DIR, "tenant-config.runtime.json");
  const candidates = [];
  if (rawJson) candidates.push({ source: "TENANT_CONFIG_JSON", text: rawJson });
  if (configFile) {
    try {
      if (fs.existsSync(configFile)) {
        candidates.push({ source: `TENANT_CONFIG_FILE:${configFile}`, text: fs.readFileSync(configFile, "utf8") });
      }
    } catch (_) {
      // Fall through.
    }
  }
  try {
    if (fs.existsSync(runtimeFile)) {
      candidates.push({
        source: `TENANT_CONFIG_RUNTIME:${runtimeFile}`,
        text: fs.readFileSync(runtimeFile, "utf8"),
      });
    }
  } catch (_) {
    // Ignore runtime file read issues.
  }
  for (const c of candidates) {
    try {
      const parsed = JSON.parse(c.text);
      return {
        source: c.source,
        dealerAliasesRaw: normalizeConfigMap(parsed?.dealerAliases),
        emailDealerMapRaw: normalizeConfigMap(parsed?.emailDealerMap),
      };
    } catch (_) {
      // Try next.
    }
  }
  return { source: "env_defaults", dealerAliasesRaw: "", emailDealerMapRaw: "" };
}

let TENANT_CONFIG = loadTenantConfig();
const AUTH_DEALER_ID_ALIASES_RAW_DEFAULT = String(process.env.AUTH_DEALER_ID_ALIASES || "").trim();
const AUTH_EMAIL_DEALER_MAP_RAW_DEFAULT = String(process.env.AUTH_EMAIL_DEALER_MAP || "").trim();
let AUTH_DEALER_ID_ALIASES_RAW = String(
  TENANT_CONFIG.dealerAliasesRaw || AUTH_DEALER_ID_ALIASES_RAW_DEFAULT || ""
).trim();
let AUTH_EMAIL_DEALER_MAP_RAW = String(
  TENANT_CONFIG.emailDealerMapRaw || AUTH_EMAIL_DEALER_MAP_RAW_DEFAULT || ""
).trim();
const TENANT_ADMIN_TOKEN = String(process.env.TENANT_ADMIN_TOKEN || "").trim();
const TENANT_CONFIG_RUNTIME_FILE = path.join(AUTH_DATA_DIR, "tenant-config.runtime.json");
const BUSINESS_ROLES = ["dealer_principal", "sales_manager", "sales_person"];
const LOGIN_MAX_FAILED_ATTEMPTS = Number(process.env.LOGIN_MAX_FAILED_ATTEMPTS || 5);
const LOGIN_WINDOW_MS = Number(process.env.LOGIN_WINDOW_MS || 15 * 60 * 1000);
const LOGIN_LOCKOUT_MS = Number(process.env.LOGIN_LOCKOUT_MS || 15 * 60 * 1000);

if (!JWT_SECRET || JWT_SECRET === "CHANGE_ME_JWT_SECRET") {
  console.warn(
    "[cubeone-auth] WARNING: Using default JWT_SECRET. Set JWT_SECRET in .env for production."
  );
}

try {
  fs.mkdirSync(AUTH_DATA_DIR, { recursive: true });
} catch (err) {
  console.error("[cubeone-auth] Failed to initialize AUTH_DATA_DIR", {
    AUTH_DATA_DIR,
    error: err?.message || String(err),
  });
}

function readUsers() {
  try {
    if (!fs.existsSync(USERS_FILE)) return [];
    const raw = fs.readFileSync(USERS_FILE, "utf8");
    const users = JSON.parse(raw);
    if (!Array.isArray(users)) return [];
    let changed = false;
    for (const u of users) {
      if (!u || typeof u !== "object") continue;
      const canonicalDealer = resolveUserDealerId(u.email, u.dealerId);
      if (canonicalDealer && String(u.dealerId || "").trim() !== canonicalDealer) {
        u.dealerId = canonicalDealer;
        changed = true;
      }
      const normalizedEmail = normalizeEmail(u.email);
      if (normalizedEmail && normalizedEmail !== String(u.email || "")) {
        u.email = normalizedEmail;
        changed = true;
      }
      const normalizedRole = normalizeRole(u.role);
      if (normalizedRole !== String(u.role || "")) {
        u.role = normalizedRole;
        changed = true;
      }
    }
    if (changed) writeUsers(users);
    return users;
  } catch (_) {
    return [];
  }
}

function writeUsers(users) {
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), "utf8");
}

function readAuditLog() {
  try {
    if (!fs.existsSync(AUDIT_FILE)) return [];
    const raw = fs.readFileSync(AUDIT_FILE, "utf8");
    return JSON.parse(raw);
  } catch (_) {
    return [];
  }
}

function appendAudit(entry) {
  const now = new Date().toISOString();
  const base = {
    id: `evt_${uuidv4()}`,
    ts: now,
  };
  const existing = readAuditLog();
  existing.push({ ...base, ...entry });
  fs.writeFileSync(AUDIT_FILE, JSON.stringify(existing.slice(-5000), null, 2), "utf8");
}

function readSessionState() {
  try {
    if (!fs.existsSync(SESSION_FILE)) {
      return { revokedJtis: [], failedLoginByKey: {}, userLockUntil: {} };
    }
    const raw = fs.readFileSync(SESSION_FILE, "utf8");
    const parsed = JSON.parse(raw);
    return {
      revokedJtis: Array.isArray(parsed.revokedJtis) ? parsed.revokedJtis : [],
      failedLoginByKey: parsed.failedLoginByKey && typeof parsed.failedLoginByKey === "object" ? parsed.failedLoginByKey : {},
      userLockUntil: parsed.userLockUntil && typeof parsed.userLockUntil === "object" ? parsed.userLockUntil : {},
    };
  } catch (_) {
    return { revokedJtis: [], failedLoginByKey: {}, userLockUntil: {} };
  }
}

function writeSessionState(state) {
  fs.writeFileSync(SESSION_FILE, JSON.stringify(state, null, 2), "utf8");
}

function clearUserLock(userId) {
  if (!userId) return;
  const state = readSessionState();
  if (state.userLockUntil && Object.prototype.hasOwnProperty.call(state.userLockUntil, userId)) {
    delete state.userLockUntil[userId];
    writeSessionState(state);
  }
}

function recordFailedLogin(loginKey, userId) {
  const now = Date.now();
  const state = readSessionState();
  const current = state.failedLoginByKey[loginKey];
  if (!current || now - Number(current.firstTs || 0) > LOGIN_WINDOW_MS) {
    state.failedLoginByKey[loginKey] = { count: 1, firstTs: now, lastTs: now };
  } else {
    state.failedLoginByKey[loginKey] = {
      count: Number(current.count || 0) + 1,
      firstTs: Number(current.firstTs || now),
      lastTs: now,
    };
  }
  const failed = state.failedLoginByKey[loginKey];
  if (userId && Number(failed.count || 0) >= LOGIN_MAX_FAILED_ATTEMPTS) {
    state.userLockUntil[userId] = now + LOGIN_LOCKOUT_MS;
  }
  writeSessionState(state);
  return failed;
}

function clearFailedLogin(loginKey, userId) {
  const state = readSessionState();
  delete state.failedLoginByKey[loginKey];
  if (userId) delete state.userLockUntil[userId];
  writeSessionState(state);
}

function getLockUntil(userId) {
  if (!userId) return 0;
  const state = readSessionState();
  const ts = Number(state.userLockUntil[userId] || 0);
  return Number.isFinite(ts) ? ts : 0;
}

function passwordMeetsPolicy(pw) {
  const s = String(pw || "");
  if (s.length < 10) return false;
  if (!/[A-Za-z]/.test(s)) return false;
  if (!/[0-9]/.test(s)) return false;
  return true;
}

function revokeJti(jti, expSeconds) {
  if (!jti) return;
  const state = readSessionState();
  const nowMs = Date.now();
  const expMs = Number(expSeconds || 0) * 1000;
  state.revokedJtis = (state.revokedJtis || [])
    .filter((r) => Number(r?.expMs || 0) > nowMs)
    .filter((r) => String(r?.jti || "") !== String(jti));
  state.revokedJtis.push({ jti: String(jti), expMs: Number.isFinite(expMs) ? expMs : nowMs + 60 * 60 * 1000 });
  writeSessionState(state);
}

function isRevokedJti(jti) {
  if (!jti) return false;
  const nowMs = Date.now();
  const state = readSessionState();
  const pruned = (state.revokedJtis || []).filter((r) => Number(r?.expMs || 0) > nowMs);
  if (pruned.length !== (state.revokedJtis || []).length) {
    state.revokedJtis = pruned;
    writeSessionState(state);
  }
  return pruned.some((r) => String(r?.jti || "") === String(jti));
}

function normalizeRole(input) {
  const raw = String(input || "").trim().toLowerCase();
  if (!raw) return "sales_person";
  if (BUSINESS_ROLES.includes(raw)) return raw;
  // Backward compatibility mapping for older roles.
  if (["superadmin", "owner", "admin"].includes(raw)) return "dealer_principal";
  if (raw === "agent") return "sales_person";
  return "sales_person";
}

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

let AUTH_DEALER_ID_ALIASES = parseMap(AUTH_DEALER_ID_ALIASES_RAW);
let AUTH_EMAIL_DEALER_MAP = parseMap(AUTH_EMAIL_DEALER_MAP_RAW);

function tenantConfigSnapshot() {
  return {
    source: TENANT_CONFIG.source,
    dealerAliases: Object.fromEntries(AUTH_DEALER_ID_ALIASES),
    emailDealerMap: Object.fromEntries(AUTH_EMAIL_DEALER_MAP),
  };
}

function validateTenantConfigPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "tenant config payload must be a JSON object";
  }
  const maybeMapKeys = ["dealerAliases", "emailDealerMap"];
  for (const key of maybeMapKeys) {
    const value = payload[key];
    if (value == null) continue;
    if (typeof value !== "object" || Array.isArray(value)) {
      return `${key} must be an object map`;
    }
  }
  return null;
}

function applyTenantConfigPayload(payload, sourceLabel = "admin_api") {
  TENANT_CONFIG = {
    source: sourceLabel,
    dealerAliasesRaw: normalizeConfigMap(payload?.dealerAliases),
    emailDealerMapRaw: normalizeConfigMap(payload?.emailDealerMap),
  };
  AUTH_DEALER_ID_ALIASES_RAW = String(
    TENANT_CONFIG.dealerAliasesRaw || AUTH_DEALER_ID_ALIASES_RAW_DEFAULT || ""
  ).trim();
  AUTH_EMAIL_DEALER_MAP_RAW = String(
    TENANT_CONFIG.emailDealerMapRaw || AUTH_EMAIL_DEALER_MAP_RAW_DEFAULT || ""
  ).trim();
  AUTH_DEALER_ID_ALIASES = parseMap(AUTH_DEALER_ID_ALIASES_RAW);
  AUTH_EMAIL_DEALER_MAP = parseMap(AUTH_EMAIL_DEALER_MAP_RAW);
}

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

function sanitizeUser(user) {
  const state = readSessionState();
  const rawLock =
    state.userLockUntil && user.userId ? state.userLockUntil[String(user.userId)] : undefined;
  const lockedUntil =
    typeof rawLock === "number" && Number.isFinite(rawLock) && rawLock > 0
      ? new Date(rawLock).toISOString()
      : null;
  return {
    userId: user.userId || "",
    email: user.email || "",
    dealerId: resolveUserDealerId(user.email, user.dealerId),
    branchId: user.branchId || "branch_default",
    role: normalizeRole(user.role),
    active: user.active !== false,
    createdAt: user.createdAt || null,
    mustChangePassword: user.mustChangePassword === true,
    lockedUntil,
  };
}

function verifyAccessToken(req, res, next) {
  const header = String(req.headers.authorization || "");
  const m = header.match(/^Bearer\s+(.+)$/i);
  if (!m) return res.status(401).json({ error: "missing_bearer_token" });
  try {
    const claims = jwt.verify(m[1], JWT_SECRET);
    const users = readUsers();
    const idx = users.findIndex((u) => String(u.userId || "") === String(claims.userId || ""));
    if (idx < 0) return res.status(401).json({ error: "invalid_token_subject" });
    const user = users[idx];
    if (user.active === false) return res.status(403).json({ error: "user_inactive" });
    if (isRevokedJti(claims.jti)) return res.status(401).json({ error: "token_revoked" });
    const tokenSessionVersion = Number(claims.sv || 1);
    const userSessionVersion = Number(user.sessionVersion || 1);
    if (tokenSessionVersion !== userSessionVersion) {
      return res.status(401).json({ error: "session_version_mismatch" });
    }
    const canonicalDealer = resolveUserDealerId(user.email, user.dealerId);
    req.authClaims = {
      ...claims,
      email: normalizeEmail(user.email || claims.email || ""),
      dealerId: canonicalDealer,
      role: normalizeRole(user.role || claims.role),
      userId: user.userId || claims.userId || claims.sub || "",
    };
    req.authUser = { ...user, dealerId: canonicalDealer };
    next();
  } catch (_) {
    return res.status(401).json({ error: "invalid_token" });
  }
}

function requireAuth(req, res, next) {
  return verifyAccessToken(req, res, next);
}

function requireTenantAdmin(req, res, next) {
  const header = String(req.headers.authorization || "");
  const m = header.match(/^Bearer\s+(.+)$/i);
  if (!m) return res.status(401).json({ error: "missing_bearer_token" });
  try {
    const claims = jwt.verify(m[1], JWT_SECRET);
    const role = normalizeRole(claims?.role);
    if (role !== "dealer_principal") {
      return res.status(403).json({ error: "forbidden", hint: "dealer_principal_required" });
    }
    if (TENANT_ADMIN_TOKEN) {
      const provided = String(req.headers["x-tenant-admin-token"] || "").trim();
      if (!provided || provided !== TENANT_ADMIN_TOKEN) {
        return res.status(403).json({ error: "forbidden", hint: "missing_or_invalid_tenant_admin_token" });
      }
    }
    req.authClaims = claims;
    next();
  } catch (_) {
    return res.status(401).json({ error: "invalid_token" });
  }
}

function requireRoleSet(allowedRoles) {
  return (req, res, next) =>
    verifyAccessToken(req, res, () => {
      const role = normalizeRole(req.authClaims?.role);
      if (!allowedRoles.includes(role)) return res.status(403).json({ error: "forbidden" });
      next();
    });
}

const requireAdmin = requireRoleSet(["dealer_principal", "sales_manager"]);
const requireDealerUser = requireRoleSet(["dealer_principal", "sales_manager", "sales_person"]);

function getScopedDealerIdFromClaims(claims) {
  return canonicalDealerId(claims?.dealerId || "");
}

function resolveClientIp(req) {
  const raw = String(req.headers["x-forwarded-for"] || req.socket?.remoteAddress || "")
    .split(",")[0]
    .trim();
  return raw || "unknown_ip";
}

app.get("/healthz", (_req, res) => {
  res.json({ ok: true });
});

app.get("/api/v1/admin/tenant-config", requireTenantAdmin, (_req, res) => {
  return res.json({
    ok: true,
    ...tenantConfigSnapshot(),
  });
});

app.put("/api/v1/admin/tenant-config", requireTenantAdmin, (req, res) => {
  const payload = req.body || {};
  const validationError = validateTenantConfigPayload(payload);
  if (validationError) {
    return res.status(400).json({ error: "invalid_tenant_config", detail: validationError });
  }
  applyTenantConfigPayload(payload, "admin_api_runtime");
  try {
    fs.mkdirSync(AUTH_DATA_DIR, { recursive: true });
    fs.writeFileSync(TENANT_CONFIG_RUNTIME_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (e) {
    return res.status(500).json({
      error: "tenant_config_persist_failed",
      detail: e?.message || String(e),
      appliedInMemory: true,
    });
  }
  return res.json({
    ok: true,
    savedTo: TENANT_CONFIG_RUNTIME_FILE,
    ...tenantConfigSnapshot(),
  });
});

app.post("/api/v1/auth/register", async (req, res) => {
  const { email, password, dealerId, branchId, role } = req.body || {};
  if (!email || !password) {
    return res.status(400).json({ error: "email and password are required" });
  }
  if (!passwordMeetsPolicy(password)) {
    return res.status(400).json({
      error: "weak_password",
      message: "Password must be at least 10 characters and contain letters and numbers (min 10).",
    });
  }

  const users = readUsers();
  const existing = users.find((u) => u.email.toLowerCase() === email.toLowerCase());
  if (existing) {
    return res.status(409).json({ error: "user_exists" });
  }

  const passwordHash = await bcrypt.hash(password, 12);
  users.push({
    userId: `usr_${uuidv4()}`,
    email,
    passwordHash,
    dealerId: resolveUserDealerId(email, dealerId),
    branchId: String(branchId || "branch_default"),
    role: normalizeRole(role),
    active: true,
    createdAt: new Date().toISOString(),
    mustChangePassword: false,
    sessionVersion: 1,
  });
  writeUsers(users);

  appendAudit({
    type: "user_register_self",
    actorUserId: null,
    actorEmail: email,
    targetUserId: null,
    targetEmail: email,
    dealerId: resolveUserDealerId(email, dealerId),
    details: { method: "self_register" },
  });

  return res.status(201).json({ ok: true });
});

app.post("/api/v1/auth/login", async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) {
    return res.status(400).json({ error: "username and password are required" });
  }

  const normalizedUsername = String(username || "").trim().toLowerCase();
  const clientIp = resolveClientIp(req);
  const loginKey = `${normalizedUsername}|${clientIp}`;
  const users = readUsers();
  const user = users.find((u) => u.email.toLowerCase() === normalizedUsername);
  if (!user) {
    recordFailedLogin(loginKey, null);
    appendAudit({
      type: "login_failed",
      actorUserId: null,
      actorEmail: username,
      targetUserId: null,
      targetEmail: username,
      dealerId: null,
      details: { reason: "user_not_found" },
    });
    return res.status(401).json({ error: "invalid_credentials" });
  }
  const lockUntil = getLockUntil(String(user.userId || ""));
  if (lockUntil > Date.now()) {
    appendAudit({
      type: "login_blocked_lockout",
      actorUserId: user.userId || null,
      actorEmail: user.email,
      targetUserId: user.userId || null,
      targetEmail: user.email,
      dealerId: resolveUserDealerId(user.email, user.dealerId) || null,
      details: { lockUntil: new Date(lockUntil).toISOString(), ip: clientIp },
    });
    return res.status(423).json({ error: "account_locked", lockUntil: new Date(lockUntil).toISOString() });
  }
  if (user.active === false) {
    appendAudit({
      type: "login_blocked_inactive",
      actorUserId: user.userId || null,
      actorEmail: user.email,
      targetUserId: user.userId || null,
      targetEmail: user.email,
      dealerId: user.dealerId || null,
      details: {},
    });
    return res.status(403).json({ error: "user_inactive" });
  }

  const ok = await bcrypt.compare(password, user.passwordHash);
  if (!ok) {
    const failed = recordFailedLogin(loginKey, String(user.userId || ""));
    appendAudit({
      type: "login_failed",
      actorUserId: user.userId || null,
      actorEmail: user.email,
      targetUserId: user.userId || null,
      targetEmail: user.email,
      dealerId: user.dealerId || null,
      details: {
        reason: "bad_password",
        ip: clientIp,
        failedAttempts: Number(failed?.count || 1),
      },
    });
    return res.status(401).json({ error: "invalid_credentials" });
  }
  clearFailedLogin(loginKey, String(user.userId || ""));

  const canonicalDealerIdForUser = resolveUserDealerId(user.email, user.dealerId);
  if (canonicalDealerIdForUser !== String(user.dealerId || "").trim()) {
    user.dealerId = canonicalDealerIdForUser;
    writeUsers(users);
  }

  const accessToken = jwt.sign(
    {
      sub: user.email,
      email: user.email,
      userId: user.userId || `usr_${Buffer.from(user.email).toString("hex").slice(0, 10)}`,
      dealerId: canonicalDealerIdForUser,
      branchId: user.branchId || "branch_default",
      role: normalizeRole(user.role),
      sv: Number(user.sessionVersion || 1),
    },
    JWT_SECRET,
    { expiresIn: "1h", jwtid: `jti_${uuidv4()}` }
  );

  appendAudit({
    type: "login_success",
    actorUserId: user.userId || null,
    actorEmail: user.email,
    targetUserId: user.userId || null,
    targetEmail: user.email,
    dealerId: canonicalDealerIdForUser || null,
    details: { ip: clientIp },
  });

  return res.json({
    access_token: accessToken,
    token_type: "bearer",
    user: {
      userId: user.userId || "",
      email: user.email,
      dealerId: canonicalDealerIdForUser,
      branchId: user.branchId || "branch_default",
      role: normalizeRole(user.role),
      mustChangePassword: user.mustChangePassword === true,
    },
  });
});

app.post("/api/v1/auth/logout", requireAuth, (req, res) => {
  revokeJti(req.authClaims?.jti, req.authClaims?.exp);
  appendAudit({
    type: "logout_current_session",
    actorUserId: req.authUser?.userId || null,
    actorEmail: req.authUser?.email || null,
    targetUserId: req.authUser?.userId || null,
    targetEmail: req.authUser?.email || null,
    dealerId: req.authUser?.dealerId || null,
    details: {},
  });
  return res.json({ ok: true });
});

app.post("/api/v1/auth/logout-all", requireAuth, (req, res) => {
  const userId = String(req.authUser?.userId || "").trim();
  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId || "").trim() === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  users[idx].sessionVersion = Number(users[idx].sessionVersion || 1) + 1;
  writeUsers(users);
  revokeJti(req.authClaims?.jti, req.authClaims?.exp);
  appendAudit({
    type: "logout_all_sessions",
    actorUserId: users[idx].userId || null,
    actorEmail: users[idx].email || null,
    targetUserId: users[idx].userId || null,
    targetEmail: users[idx].email || null,
    dealerId: users[idx].dealerId || null,
    details: { newSessionVersion: users[idx].sessionVersion },
  });
  return res.json({ ok: true });
});

app.get("/api/v1/admin/users", requireAdmin, (_req, res) => {
  const scopedDealerId = getScopedDealerIdFromClaims(_req.authClaims);
  if (!scopedDealerId) {
    return res.status(403).json({ error: "missing_dealer_scope" });
  }
  const users = readUsers()
    .filter((u) => resolveUserDealerId(u.email, u.dealerId) === scopedDealerId)
    .map(sanitizeUser);
  return res.json({ users, count: users.length });
});

app.post("/api/v1/admin/users", requireAdmin, async (req, res) => {
  const { email, password, dealerId, branchId, role } = req.body || {};
  if (!email || !password) {
    return res.status(400).json({ error: "email and password are required" });
  }
  const users = readUsers();
  const existing = users.find((u) => u.email.toLowerCase() === String(email).toLowerCase());
  if (existing) return res.status(409).json({ error: "user_exists" });

  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) {
    return res.status(403).json({ error: "missing_dealer_scope" });
  }
  const requestedDealerId = String(dealerId || "").trim();
  if (requestedDealerId && canonicalDealerId(requestedDealerId) !== scopedDealerId) {
    return res.status(403).json({ error: "cross_dealer_user_create_forbidden" });
  }

  const passwordHash = await bcrypt.hash(password, 12);
  const newUser = {
    userId: `usr_${uuidv4()}`,
    email: String(email).trim(),
    passwordHash,
    dealerId: resolveUserDealerId(email, scopedDealerId),
    branchId: String(branchId || "branch_default"),
    role: normalizeRole(role),
    active: true,
    createdAt: new Date().toISOString(),
    mustChangePassword: true,
    sessionVersion: 1,
  };
  users.push(newUser);
  writeUsers(users);

  appendAudit({
    type: "admin_user_created",
    actorUserId: req.authClaims?.userId || null,
    actorEmail: req.authClaims?.email || null,
    targetUserId: newUser.userId,
    targetEmail: newUser.email,
    dealerId: scopedDealerId,
    details: { role: newUser.role },
  });
  return res.status(201).json({ ok: true, user: sanitizeUser(newUser) });
});

app.patch("/api/v1/admin/users/:userId/role", requireAdmin, (req, res) => {
  const userId = String(req.params.userId || "").trim();
  const role = String(req.body?.role || "").trim().toLowerCase();
  if (!userId || !role) return res.status(400).json({ error: "userId and role are required" });
  const allowedRoles = BUSINESS_ROLES;
  if (!allowedRoles.includes(role)) return res.status(400).json({ error: "invalid_role" });

  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId) === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) return res.status(403).json({ error: "missing_dealer_scope" });
  if (resolveUserDealerId(users[idx].email, users[idx].dealerId) !== scopedDealerId) {
    return res.status(403).json({ error: "cross_dealer_role_change_forbidden" });
  }
  users[idx].role = role;
  writeUsers(users);

  appendAudit({
    type: "admin_user_role_changed",
    actorUserId: req.authClaims?.userId || null,
    actorEmail: req.authClaims?.email || null,
    targetUserId: users[idx].userId,
    targetEmail: users[idx].email,
    dealerId: scopedDealerId,
    details: { newRole: role },
  });
  return res.json({ ok: true, user: sanitizeUser(users[idx]) });
});

app.patch("/api/v1/admin/users/:userId/status", requireAdmin, (req, res) => {
  const userId = String(req.params.userId || "").trim();
  const active = Boolean(req.body?.active);
  if (!userId) return res.status(400).json({ error: "userId is required" });

  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId) === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) return res.status(403).json({ error: "missing_dealer_scope" });
  if (resolveUserDealerId(users[idx].email, users[idx].dealerId) !== scopedDealerId) {
    return res.status(403).json({ error: "cross_dealer_status_change_forbidden" });
  }
  if (active === false && String(req.authClaims?.userId || "").trim() === userId) {
    return res.status(400).json({ error: "cannot_deactivate_self" });
  }
  users[idx].active = active;
  writeUsers(users);

  appendAudit({
    type: "admin_user_status_changed",
    actorUserId: req.authClaims?.userId || null,
    actorEmail: req.authClaims?.email || null,
    targetUserId: users[idx].userId,
    targetEmail: users[idx].email,
    dealerId: scopedDealerId,
    details: { active },
  });
  return res.json({ ok: true, user: sanitizeUser(users[idx]) });
});

app.post("/api/v1/admin/users/:userId/reset-password", requireAdmin, async (req, res) => {
  const userId = String(req.params.userId || "").trim();
  const newPassword = String(req.body?.newPassword || "");
  if (!userId || !newPassword) {
    return res.status(400).json({ error: "userId and newPassword are required" });
  }
  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId) === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) return res.status(403).json({ error: "missing_dealer_scope" });
  if (resolveUserDealerId(users[idx].email, users[idx].dealerId) !== scopedDealerId) {
    return res.status(403).json({ error: "cross_dealer_password_reset_forbidden" });
  }
  if (!passwordMeetsPolicy(newPassword)) {
    return res.status(400).json({
      error: "weak_password",
      message: "Password must be at least 10 characters and contain letters and numbers (min 10).",
    });
  }
  const sameAsOld = await bcrypt.compare(String(newPassword), users[idx].passwordHash);
  if (sameAsOld) {
    return res.status(400).json({ error: "password_reuse_not_allowed" });
  }
  users[idx].passwordHash = await bcrypt.hash(newPassword, 12);
  users[idx].mustChangePassword = true;
  writeUsers(users);

  appendAudit({
    type: "admin_user_password_reset",
    actorUserId: req.authClaims?.userId || null,
    actorEmail: req.authClaims?.email || null,
    targetUserId: users[idx].userId,
    targetEmail: users[idx].email,
    dealerId: scopedDealerId,
    details: {},
  });
  return res.json({ ok: true, user: sanitizeUser(users[idx]) });
});

app.post("/api/v1/admin/users/:userId/unlock", requireAdmin, (req, res) => {
  const userId = String(req.params.userId || "").trim();
  if (!userId) return res.status(400).json({ error: "userId is required" });
  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId) === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) return res.status(403).json({ error: "missing_dealer_scope" });
  if (resolveUserDealerId(users[idx].email, users[idx].dealerId) !== scopedDealerId) {
    return res.status(403).json({ error: "cross_dealer_unlock_forbidden" });
  }
  clearUserLock(String(users[idx].userId || ""));
  appendAudit({
    type: "admin_user_unlocked",
    actorUserId: req.authClaims?.userId || null,
    actorEmail: req.authClaims?.email || null,
    targetUserId: users[idx].userId,
    targetEmail: users[idx].email,
    dealerId: scopedDealerId,
    details: {},
  });
  return res.json({ ok: true, user: sanitizeUser(users[idx]) });
});

app.post("/api/v1/auth/change-password", requireAuth, async (req, res) => {
  const userId = String(req.authClaims?.userId || "").trim();
  const { currentPassword, newPassword } = req.body || {};
  if (!userId || !currentPassword || !newPassword) {
    return res.status(400).json({ error: "userId,currentPassword,newPassword are required" });
  }
  const users = readUsers();
  const idx = users.findIndex((u) => String(u.userId) === userId);
  if (idx < 0) return res.status(404).json({ error: "not_found" });
  const user = users[idx];
  const ok = await bcrypt.compare(currentPassword, user.passwordHash);
  if (!ok) {
    appendAudit({
      type: "password_change_failed",
      actorUserId: user.userId || null,
      actorEmail: user.email,
      targetUserId: user.userId || null,
      targetEmail: user.email,
      dealerId: user.dealerId || null,
      details: { reason: "bad_current_password" },
    });
    return res.status(401).json({ error: "invalid_current_password" });
  }
  if (!passwordMeetsPolicy(newPassword)) {
    return res.status(400).json({
      error: "weak_password",
      message: "Password must be at least 10 characters and contain letters and numbers (min 10).",
    });
  }
  const sameAsOld = await bcrypt.compare(String(newPassword), user.passwordHash);
  if (sameAsOld) {
    return res.status(400).json({ error: "password_reuse_not_allowed" });
  }
  users[idx].passwordHash = await bcrypt.hash(String(newPassword), 12);
  users[idx].mustChangePassword = false;
  writeUsers(users);

  appendAudit({
    type: "password_changed",
    actorUserId: user.userId || null,
    actorEmail: user.email,
    targetUserId: user.userId || null,
    targetEmail: user.email,
    dealerId: resolveUserDealerId(user.email, user.dealerId) || null,
    details: {},
  });
  return res.json({ ok: true });
});

app.get("/api/v1/admin/audit-events", requireDealerUser, (req, res) => {
  const scopedDealerId = getScopedDealerIdFromClaims(req.authClaims);
  if (!scopedDealerId) return res.status(403).json({ error: "missing_dealer_scope" });
  const limitRaw = Number(req.query?.limit || 100);
  const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 500) : 100;
  const actionFilter = String(req.query?.action || "").trim().toLowerCase();
  const userFilter = String(req.query?.user || "").trim().toLowerCase();
  const events = readAuditLog()
    .filter((evt) => canonicalDealerId(evt.dealerId) === scopedDealerId)
    .filter((evt) => {
      if (!actionFilter) return true;
      return String(evt.type || "").toLowerCase().includes(actionFilter);
    })
    .filter((evt) => {
      if (!userFilter) return true;
      const actor = String(evt.actorEmail || "").toLowerCase();
      const target = String(evt.targetEmail || "").toLowerCase();
      return actor.includes(userFilter) || target.includes(userFilter);
    })
    .slice(-limit)
    .reverse();
  return res.json({ count: events.length, events });
});

app.listen(PORT, () => {
  console.log(`cubeone-auth listening on http://0.0.0.0:${PORT}`);
  console.log(`[cubeone-auth] data dir: ${AUTH_DATA_DIR}`);
  console.log(
    `[cubeone-auth] dealer aliases: ${AUTH_DEALER_ID_ALIASES.size}, email dealer overrides: ${AUTH_EMAIL_DEALER_MAP.size}, tenant config source: ${TENANT_CONFIG.source}`
  );
});

