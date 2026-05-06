require("dotenv").config();

const express = require("express");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { createEvolvesaProvider } = require("./providers/evolvesa");
let jwt = null;
try {
  jwt = require("jsonwebtoken");
} catch (_) {
  // Optional dependency; tenant verification will be disabled if missing.
}

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use(express.urlencoded({ extended: false }));
const STATIC_PUBLIC_DIR = path.join(__dirname, "public");
app.use(express.static(STATIC_PUBLIC_DIR));

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

function normalizeConfigList(raw) {
  if (!raw) return "";
  if (typeof raw === "string") return normalizeConfigToken(raw);
  if (Array.isArray(raw)) return raw.map((x) => normalizeConfigToken(x)).filter(Boolean).join(",");
  return "";
}

function loadTenantConfig() {
  const rawJson = String(process.env.TENANT_CONFIG_JSON || "").trim();
  const configFile = String(process.env.TENANT_CONFIG_FILE || "").trim();
  const runtimeFile = path.join(__dirname, "data", "tenant-config.runtime.json");
  const candidates = [];
  if (rawJson) candidates.push({ source: "TENANT_CONFIG_JSON", text: rawJson });
  if (configFile) {
    try {
      if (fs.existsSync(configFile)) {
        candidates.push({ source: `TENANT_CONFIG_FILE:${configFile}`, text: fs.readFileSync(configFile, "utf8") });
      }
    } catch (_) {
      // Fall through to env defaults.
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
    // Ignore runtime file read issues and continue to env defaults.
  }
  for (const c of candidates) {
    try {
      const parsed = JSON.parse(c.text);
      return {
        source: c.source,
        dealerAliasesRaw: normalizeConfigMap(parsed?.dealerAliases),
        userEmailDealerMapRaw: normalizeConfigMap(parsed?.emailDealerMap),
        vmgDealerScopesRaw: normalizeConfigList(parsed?.vmgDealerScopes),
        evolvesaLeadReceivingEntityMapRaw: normalizeConfigMap(parsed?.evolvesaLeadReceivingEntityMap),
        evolvesaLeadTriggerUrlByDealerRaw: normalizeConfigMap(parsed?.evolvesaLeadTriggerUrlByDealer),
        vmgStockFeedUrlByDealerRaw: normalizeConfigMap(parsed?.vmgStockFeedUrlByDealer),
      };
    } catch (_) {
      // Try next candidate.
    }
  }
  return {
    source: "env_defaults",
    dealerAliasesRaw: "",
    userEmailDealerMapRaw: "",
    vmgDealerScopesRaw: "",
    evolvesaLeadReceivingEntityMapRaw: "",
    evolvesaLeadTriggerUrlByDealerRaw: "",
    vmgStockFeedUrlByDealerRaw: "",
  };
}

let TENANT_CONFIG = loadTenantConfig();

const PORT = process.env.PORT || 8080;
const API_KEY = process.env.API_KEY || "change-me";
const EVOLVESA_BASE_URL = (process.env.EVOLVESA_BASE_URL || "").trim().replace(/\/$/, "");
const EVOLVESA_API_KEY = (process.env.EVOLVESA_API_KEY || "").trim();
const EVOLVESA_TIMEOUT_MS = Number(process.env.EVOLVESA_TIMEOUT_MS || 15000);
const EVOLVESA_STOCK_ENDPOINT = (process.env.EVOLVESA_STOCK_ENDPOINT || "/api/v1/stocks").trim();
const EVOLVESA_STOCK_TRIGGER_URL = (process.env.EVOLVESA_STOCK_TRIGGER_URL || "").trim();
const EVOLVESA_LEAD_TRIGGER_URL = (process.env.EVOLVESA_LEAD_TRIGGER_URL || "").trim();
const EVOLVESA_LEAD_RECEIVING_ENTITY_ID = (process.env.EVOLVESA_LEAD_RECEIVING_ENTITY_ID || "505").trim();
const EVOLVESA_LEAD_RECEIVING_ENTITY_NAME = (process.env.EVOLVESA_LEAD_RECEIVING_ENTITY_NAME || "LDC").trim();
const EVOLVESA_LEAD_RECEIVING_ENTITY_MAP_RAW = (
  TENANT_CONFIG.evolvesaLeadReceivingEntityMapRaw ||
  process.env.EVOLVESA_LEAD_RECEIVING_ENTITY_MAP ||
  ""
).trim();
const EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER_RAW = (
  TENANT_CONFIG.evolvesaLeadTriggerUrlByDealerRaw ||
  process.env.EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER ||
  ""
).trim();
const EVOLVESA_LEAD_SOURCE = (process.env.EVOLVESA_LEAD_SOURCE || "CubeOneScan").trim();
const EVOLVESA_LEAD_SOURCE_ID = (process.env.EVOLVESA_LEAD_SOURCE_ID || "").trim();
const EVOLVESA_STOCK_SOURCE = (process.env.EVOLVESA_STOCK_SOURCE || "CubeOneScan").trim();
const EVOLVESA_STOCK_SOURCE_ID = (process.env.EVOLVESA_STOCK_SOURCE_ID || "").trim();
const EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA = (process.env.EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA || "Gauteng").trim();
const EVOLVESA_DEFAULT_LEAD_USER_AREA = (process.env.EVOLVESA_DEFAULT_LEAD_USER_AREA || "JHB").trim();
const DEFAULT_CRM_PROVIDER = (process.env.DEFAULT_CRM_PROVIDER || "evolvesa").trim().toLowerCase();
const CRM_PROVIDER_BY_DEALER_RAW = (process.env.CRM_PROVIDER_BY_DEALER || "").trim();
const AUTOTRADER_LISTINGS_URL = (
  process.env.AUTOTRADER_LISTINGS_URL ||
  "https://services.autotrader.co.za/api/syndication/v1.0/listings"
).trim();
const AUTOTRADER_BASIC_AUTH = (process.env.AUTOTRADER_BASIC_AUTH || "").trim();
const AUTOTRADER_TIMEOUT_MS = Number(process.env.AUTOTRADER_TIMEOUT_MS || 12000);
const AUTOTRADER_LISTINGS_CACHE_TTL_MS = Number(process.env.AUTOTRADER_LISTINGS_CACHE_TTL_MS || 300000);
const AUTOTRADER_RATE_LIMIT_COOLDOWN_MS = Number(process.env.AUTOTRADER_RATE_LIMIT_COOLDOWN_MS || 180000);
const VMG_STOCK_FEED_URL = (process.env.VMG_STOCK_FEED_URL || "").trim();
// Optional: dealerId=feedUrl,... so each VMG dealer can use its own XML feed (e.g. DriveX 510 vs LDC 1257).
const VMG_STOCK_FEED_URL_BY_DEALER_RAW = (
  TENANT_CONFIG.vmgStockFeedUrlByDealerRaw ||
  process.env.VMG_STOCK_FEED_URL_BY_DEALER ||
  ""
).trim();
// Comma-separated VMG/DMS dealer ids that use VMG_STOCK_FEED_URL (not EvolveSA portal company ids).
const VMG_DEALER_SCOPES_RAW = (TENANT_CONFIG.vmgDealerScopesRaw || process.env.VMG_DEALER_SCOPES || "").trim();
const VMG_TIMEOUT_MS = Number(process.env.VMG_TIMEOUT_MS || 12000);
const VMG_CACHE_TTL_MS = Number(process.env.VMG_CACHE_TTL_MS || 300000);
const DEALER_ID_ALIASES_RAW = (TENANT_CONFIG.dealerAliasesRaw || process.env.DEALER_ID_ALIASES || "").trim();
const USER_EMAIL_DEALER_MAP_RAW = (
  TENANT_CONFIG.userEmailDealerMapRaw ||
  process.env.USER_EMAIL_DEALER_MAP ||
  ""
).trim();
const ENFORCE_TENANT_RINGFENCE = String(process.env.ENFORCE_TENANT_RINGFENCE || "1").trim() !== "0";
const AUTH_JWT_SECRET = (process.env.AUTH_JWT_SECRET || "").trim();
const CONSENT_TOKEN_SECRET = (
  process.env.CONSENT_TOKEN_SECRET ||
  AUTH_JWT_SECRET ||
  process.env.API_KEY ||
  "consent-dev-secret"
).trim();
const CONSENT_LINK_BASE_URL = (process.env.CONSENT_LINK_BASE_URL || "").trim();
const CONSENT_DEFAULT_EXPIRY_HOURS = Number(process.env.CONSENT_DEFAULT_EXPIRY_HOURS || 24);
/** Max consent audit events kept in commands-store.json (append-only tail). */
const CONSENT_EVENTS_STORE_CAP = Math.max(
  5000,
  Math.min(200_000, Number(process.env.CONSENT_EVENTS_STORE_CAP || 50_000) || 50_000)
);
const SMTP_HOST = (process.env.SMTP_HOST || "").trim();
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_SECURE = String(process.env.SMTP_SECURE || "false").trim().toLowerCase() === "true";
const SMTP_USER = (process.env.SMTP_USER || "").trim();
const SMTP_PASS = (process.env.SMTP_PASS || "").trim();
const SMTP_FROM_EMAIL = (process.env.SMTP_FROM_EMAIL || "").trim();
/** Brevo REST API (HTTPS :443). Use on Render free tier — outbound SMTP ports are blocked (ETIMEDOUT). */
const BREVO_API_KEY = (process.env.BREVO_API_KEY || "").trim();
const CONSENT_FROM_EMAIL_BY_DEALER_RAW = (process.env.CONSENT_FROM_EMAIL_BY_DEALER || "").trim();
const CONSENT_APPROVAL_FOLLOWUP_EMAIL_ENABLED =
  String(process.env.CONSENT_APPROVAL_FOLLOWUP_EMAIL_ENABLED || "0").trim() === "1";
const CONSENT_POPIA_PRIVACY_URL = (
  process.env.CONSENT_POPIA_PRIVACY_URL ||
  "https://inforegulator.org.za/wp-content/uploads/2020/07/InfoRegSA-eForm-PriorAuthorisation-20210311.pdf"
).trim();
const CONSENT_POPIA_NOTICE_URL = (
  process.env.CONSENT_POPIA_NOTICE_URL ||
  "https://inforegulator.org.za/wp-content/uploads/2020/07/FORM-4-APPLICATION-FOR-THE-CONSENT-OF-A-DATA-SUBJECT-FOR-THE-PROCESSING-OF.pdf"
).trim();
const CONSENT_POPIA_TERMS_URL = (
  process.env.CONSENT_POPIA_TERMS_URL ||
  "https://inforegulator.org.za/wp-content/uploads/2020/07/FORM-3-APPLICATION-FOR-THE-ISSUE-OF-A-CODE-OF-CONDUCT.pdf"
).trim();
const COMMAND_MAX_RETRIES = Number(process.env.COMMAND_MAX_RETRIES || 3);
const COMMAND_RETRY_DELAY_MS = Number(process.env.COMMAND_RETRY_DELAY_MS || 1200);
const CREDIT_CHECK_MODE = String(process.env.CREDIT_CHECK_MODE || "stub").trim().toLowerCase();
const CONNECTOR_DATA_DIR = process.env.CONNECTOR_DATA_DIR || path.join(__dirname, "data");
const TENANT_ADMIN_TOKEN = String(process.env.TENANT_ADMIN_TOKEN || "").trim();
const TENANT_CONFIG_RUNTIME_FILE = path.join(CONNECTOR_DATA_DIR, "tenant-config.runtime.json");
const TENANT_CONFIG_HISTORY_FILE = path.join(CONNECTOR_DATA_DIR, "tenant-config.history.json");
const TENANT_CONFIG_PROPOSALS_FILE = path.join(CONNECTOR_DATA_DIR, "tenant-config.proposals.json");
const TENANT_ADMIN_EDITOR_ROLES = new Set(
  String(process.env.TENANT_ADMIN_EDITOR_ROLES || "dealer_principal,tenant_admin_editor")
    .split(",")
    .map((x) => String(x || "").trim().toLowerCase())
    .filter(Boolean)
);
const TENANT_ADMIN_APPROVER_ROLES = new Set(
  String(process.env.TENANT_ADMIN_APPROVER_ROLES || "dealer_principal,tenant_admin_approver")
    .split(",")
    .map((x) => String(x || "").trim().toLowerCase())
    .filter(Boolean)
);
const STORE_FILE = path.join(CONNECTOR_DATA_DIR, "commands-store.json");
const AUTOTRADER_CACHE_FILE = path.join(CONNECTOR_DATA_DIR, "autotrader-stock-cache.json");
const VMG_CACHE_FILE = path.join(CONNECTOR_DATA_DIR, "vmg-stock-cache.json");

if (!API_KEY || API_KEY === "change-me") {
  throw new Error("Refusing to start: set a strong API_KEY in connector .env (not 'change-me').");
}

function log(level, message, ctx = {}) {
  const payload = {
    ts: new Date().toISOString(),
    level,
    message,
    ...ctx,
  };
  // Keep logs machine-readable for production aggregation.
  console.log(JSON.stringify(payload));
}

function safeSleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function mapValuationError(err) {
  const raw = (err && (err.message || String(err))) || "Unknown valuation error";
  const lower = raw.toLowerCase();
  if (lower.includes("invalid model information")) {
    const suggestions = Array.isArray(err?.modelSuggestions) ? err.modelSuggestions : [];
    const suggestedText = suggestions.length > 0 ? ` Suggested models: ${suggestions.slice(0, 8).join(", ")}` : "";
    return {
      httpStatus: 422,
      userMessage: "Valuation provider does not recognize this model. Edit make/model to match provider catalog naming, then retry.",
      error: raw,
      hint: `Use exact catalog model text (not free-text from barcode).${suggestedText}`,
      suggestions,
    };
  }
  if (lower.includes("invalid variant information")) {
    const suggestions = Array.isArray(err?.variantSuggestions) ? err.variantSuggestions : [];
    const suggestedText = suggestions.length > 0 ? ` Suggested variants: ${suggestions.slice(0, 8).join(", ")}` : "";
    return {
      httpStatus: 422,
      userMessage: "Valuation provider does not recognize this variant. Edit variant to the exact catalog name, then retry.",
      error: raw,
      hint: `Variant names are strict; shorter/base variant names usually work better.${suggestedText}`,
      suggestions,
    };
  }
  if (lower.includes("tru-trade http 401") || lower.includes("tru-trade http 403")) {
    return {
      httpStatus: 502,
      userMessage: "Valuation credentials were rejected. Check connector .env credentials and restart connector.",
      error: raw,
    };
  }
  return {
    httpStatus: 500,
    userMessage: raw,
    error: raw,
  };
}

function evolveAuthHeaderValue() {
  if (!EVOLVESA_API_KEY) return null;
  const v = String(EVOLVESA_API_KEY).trim();
  if (!v) return null;
  if (/^Bearer\s+/i.test(v)) return v;
  return `Bearer ${v}`;
}

function truTradeBasicAuthHeaderValue() {
  const u = process.env.TRUTRADE_AUTH_USERNAME;
  const p = process.env.TRUTRADE_AUTH_PASSWORD;
  if (u && p) {
    return `Basic ${Buffer.from(`${u}:${p}`).toString("base64")}`;
  }
  const apiKey = process.env.TRUTRADE_API_KEY;
  const apiSecret = process.env.TRUTRADE_API_SECRET;
  if (!apiKey || !apiSecret) return null;
  return `Basic ${Buffer.from(`${apiKey}:${apiSecret}`).toString("base64")}`;
}

function autoTraderAuthHeaderValue() {
  const token = AUTOTRADER_BASIC_AUTH.trim();
  if (!token) return null;
  return token.startsWith("Basic ") ? token : `Basic ${token}`;
}

function pickFirstHttpImage(urls) {
  const list = Array.isArray(urls) ? urls : [];
  for (const u of list) {
    const s = String(u || "").trim();
    if (s.startsWith("http://") || s.startsWith("https://")) return s;
  }
  return null;
}

function requireAuth(req, res, next) {
  const header = req.headers["authorization"] || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  const token = match ? match[1] : null;
  if (!token || token !== API_KEY) {
    return res.status(401).json({
      error: "unauthorized",
      hint: "Authorization Bearer token must match API_KEY in connector .env (CubeOneScan Settings → API Key).",
    });
  }
  next();
}

function canonicalDealerId(value) {
  const raw = normalizeDealerToken(value);
  if (!raw) return "";
  // Purely numeric ids are canonical tenant/CRM ids (e.g. 1257, 510). Do not apply DEALER_ID_ALIASES
  // to them: comma-map entries like 510=208 are for stock-feed matching, not Evolve did= / ring-fence.
  if (/^\d+$/.test(raw)) return raw;
  const aliased = DEALER_ID_ALIASES.get(raw);
  return String(aliased || raw).trim();
}

function extractTenantContext(req, res, next) {
  const userToken = String(req.headers["x-user-token"] || "").trim();
  let claims = {};
  let jwtVerified = false;
  if (userToken && AUTH_JWT_SECRET) {
    try {
      claims = jwt.verify(userToken, AUTH_JWT_SECRET);
      jwtVerified = true;
    } catch (_) {
      claims = {};
    }
  }
  const headerDealerId = String(req.headers["x-dealer-id"] || "").trim();
  const headerBranchId = String(req.headers["x-branch-id"] || "").trim();
  const headerRole = String(req.headers["x-user-role"] || "").trim();
  const resolvedUserEmail = String(claims.email || claims.userEmail || req.headers["x-user-email"] || "")
    .trim()
    .toLowerCase();

  const mappedDealerIdRaw = resolvedUserEmail ? USER_EMAIL_DEALER_MAP.get(resolvedUserEmail) : null;
  const mappedDealerId = canonicalDealerId(mappedDealerIdRaw);
  const tokenDealerId = jwtVerified ? String(claims.dealerId || "").trim() : "";
  const headerDealerCanonical = canonicalDealerId(headerDealerId);
  const tokenDealerCanonical = canonicalDealerId(tokenDealerId);

  let effectiveDealerId = "";
  if (!ENFORCE_TENANT_RINGFENCE) {
    effectiveDealerId = mappedDealerId || headerDealerCanonical || tokenDealerCanonical || "";
  } else {
    if (jwtVerified && tokenDealerCanonical && headerDealerCanonical && headerDealerCanonical !== tokenDealerCanonical) {
      log("warn", "tenant_scope_header_token_mismatch_using_jwt", {
        path: req.path || null,
        method: req.method || null,
        headerDealerId: headerDealerCanonical,
        tokenDealerId: tokenDealerCanonical,
      });
    }

    // Verified JWT is the operational source of truth; connector USER_EMAIL_DEALER_MAP can lag auth / tenant-config.runtime.
    // If JWT cannot be verified, prefer the mapped email dealer scope over app header dealer scope.
    // This avoids false tenant_mismatch blocks caused by stale app-side cached dealer headers.
    if (jwtVerified && tokenDealerCanonical) {
      effectiveDealerId = tokenDealerCanonical;
    } else if (mappedDealerId) {
      effectiveDealerId = mappedDealerId;
    } else if (headerDealerCanonical) {
      effectiveDealerId = headerDealerCanonical;
    }

    if (
      USER_EMAIL_DEALER_MAP.size > 0 &&
      mappedDealerId &&
      effectiveDealerId &&
      mappedDealerId !== effectiveDealerId
    ) {
      if (jwtVerified && tokenDealerCanonical) {
        log("warn", "tenant_email_map_conflict_using_jwt", {
          path: req.path || null,
          method: req.method || null,
          email: resolvedUserEmail.replace(/^(.{2}).+(@.*)$/, "$1***$2"),
          mappedDealerId,
          tokenDealerId: tokenDealerCanonical,
        });
        effectiveDealerId = tokenDealerCanonical;
      } else {
        return sendApiError(
          req,
          res,
          403,
          "tenant_mismatch",
          "Connector email-to-dealer map does not match this session's dealer scope. Update USER_EMAIL_DEALER_MAP or sign in again."
        );
      }
    }

    if (!effectiveDealerId) {
      if (USER_EMAIL_DEALER_MAP.size > 0 && resolvedUserEmail && !mappedDealerId) {
        const redactedEmail = resolvedUserEmail.replace(/^(.{2}).+(@.*)$/, "$1***$2");
        return sendApiError(
          req,
          res,
          403,
          "tenant_not_mapped",
          `User ${redactedEmail} is not mapped in connector and session has no dealer scope.`
        );
      }
      if (USER_EMAIL_DEALER_MAP.size > 0 && !resolvedUserEmail && !jwtVerified && !headerDealerCanonical) {
        return sendApiError(req, res, 403, "tenant_identity_missing", "Missing user token/email in auth context.");
      }
      return sendApiError(req, res, 403, "tenant_scope_missing", "Unable to resolve dealership scope for this user.");
    }
  }

  const effectiveOrNull = effectiveDealerId || null;
  if (ENFORCE_TENANT_RINGFENCE && !effectiveOrNull) {
    return sendApiError(req, res, 403, "tenant_scope_missing", "Unable to resolve dealership scope for this user.");
  }

  req.tenantContext = {
    userId: claims.userId || claims.sub || req.headers["x-user-id"] || null,
    userEmail: resolvedUserEmail || null,
    userName: claims.name || claims.displayName || claims.fullName || req.headers["x-user-name"] || null,
    dealerId: effectiveOrNull,
    branchId: headerBranchId || claims.branchId || null,
    role: headerRole || claims.role || null,
  };
  next();
}

const BUSINESS_ROLES = ["dealer_principal", "sales_manager", "sales_person"];
const VALUATION_COMMAND = "MARKET_VALUE_REPORT";
const LEGACY_VALUATION_COMMANDS = new Set(["TRUTRADE_REPORT", "VALUATION_REPORT"]);
const SUBMITTABLE_COMMANDS = new Set([
  "CREATE_LEAD",
  "CREDIT_CHECK",
  "SHARE_LEAD",
  "LOG_COMMUNICATION",
  VALUATION_COMMAND,
  "CREATE_STOCK_UNIT",
  "SEND_STOCK_TO_LEAD",
  "TRADE_IN",
  "STOCK_TAKE",
]);
const REQUEST_TO_COMMAND = {
  TRADE_IN_REQUEST: "TRADE_IN",
  STOCK_TAKE_REQUEST: "STOCK_TAKE",
};
const LEAD_OWNERSHIP_ENFORCED_COMMANDS = new Set([
  "CREDIT_CHECK",
  "SHARE_LEAD",
  "LOG_COMMUNICATION",
  "SEND_STOCK_TO_LEAD",
]);

function normalizeRole(rawRole) {
  const role = String(rawRole || "").trim().toLowerCase();
  if (BUSINESS_ROLES.includes(role)) return role;
  if (["admin", "owner", "superadmin", "tenant_admin_editor", "tenant_admin_approver"].includes(role)) return "dealer_principal";
  if (["manager", "sales-manager", "sales manager"].includes(role)) return "sales_manager";
  if (role === "agent") return "sales_person";
  return "sales_person";
}

function canSubmitCommand(role, commandType) {
  const r = normalizeRole(role);
  if (commandType === "SEND_STOCK_TO_LEAD") return true;
  if (r === "dealer_principal") return true;
  if (r === "sales_manager") {
    return [
      "CREATE_LEAD",
      "CREDIT_CHECK",
      "SHARE_LEAD",
      "LOG_COMMUNICATION",
      VALUATION_COMMAND,
      "CREATE_STOCK_UNIT",
      "SEND_STOCK_TO_LEAD",
      "TRADE_IN",
      "STOCK_TAKE",
    ].includes(commandType);
  }
  // sales_person
  return [
    "CREATE_LEAD",
    "CREDIT_CHECK",
    "SHARE_LEAD",
    "LOG_COMMUNICATION",
    VALUATION_COMMAND,
    "CREATE_STOCK_UNIT",
    "SEND_STOCK_TO_LEAD",
    "TRADE_IN",
    "STOCK_TAKE",
  ].includes(commandType);
}

function requiresManagerApproval(role, commandType) {
  const r = normalizeRole(role);
  return r === "sales_person" && commandType === "TRADE_IN";
}

function isManagerOrPrincipal(role) {
  const r = normalizeRole(role);
  return r === "dealer_principal" || r === "sales_manager";
}

/**
 * In-memory command queue.
 * Replace with a database later; for v1 it's good enough to validate the flow end-to-end.
 */
const commands = new Map(); // correlationId -> { status, createdAt, commandType, payload, result }
const idempotencyIndex = new Map(); // idempotencyKey -> correlationId
const deadLetters = [];
const stockTakeSessions = new Map(); // sessionId -> hybrid stock take basket
const targetPlans = new Map(); // tenantKey( dealer|branch|period ) -> target plan
const alertRules = new Map(); // ruleId -> alert rule
const testDriveSessions = new Map(); // sessionId -> test-drive safety sessions
const consentRecords = new Map(); // consentId -> consent record
const consentEvents = []; // append-only audit trail
const leadOwnerByLeadId = new Map(); // leadId -> { ownerUserId, ownerEmail, ownerName, dealerId, branchId, assignedAt }
const leadOwnerByCorrelationId = new Map(); // create-lead correlationId -> lead owner
const autoTraderListingsCache = {
  fetchedAtMs: 0,
  rows: [],
};
/** Per-feed-url VMG cache (dealer-specific XML URLs). */
const vmgListingsCacheByUrl = new Map();
let autoTraderRateLimitedUntilMs = 0;

function parseTokenSet(raw) {
  const out = new Set();
  const text = String(raw || "").trim();
  if (!text) return out;
  for (const token of text.split(",")) {
    const v = token.trim().toLowerCase();
    if (v) out.add(v);
  }
  return out;
}

let VMG_DEALER_SCOPES = parseTokenSet(VMG_DEALER_SCOPES_RAW);
let VMG_STOCK_FEED_URL_BY_DEALER = parseKeyValueMap(VMG_STOCK_FEED_URL_BY_DEALER_RAW);

function vmgDealerHasDedicatedFeedUrl(dealerScope) {
  const ds = String(dealerScope || "").trim();
  if (!ds) return false;
  const raw = ds.toLowerCase();
  const digits = raw.replace(/\D+/g, "");
  for (const [k, v] of VMG_STOCK_FEED_URL_BY_DEALER) {
    const nk = String(k).trim().toLowerCase();
    const nd = nk.replace(/\D+/g, "");
    if ((nk === raw || (digits && nd === digits)) && String(v || "").trim()) return true;
  }
  return false;
}

function resolveVmgStockFeedUrl(dealerScope) {
  const ds = String(dealerScope || "").trim();
  if (ds) {
    const raw = ds.toLowerCase();
    const digits = raw.replace(/\D+/g, "");
    for (const [k, v] of VMG_STOCK_FEED_URL_BY_DEALER) {
      const nk = String(k).trim().toLowerCase();
      const nd = nk.replace(/\D+/g, "");
      if (nk === raw || (digits && nd === digits)) {
        const url = String(v || "").trim();
        if (url) return url;
      }
    }
  }
  return (VMG_STOCK_FEED_URL || "").trim();
}

function useVmgFeedForDealer(dealerScope) {
  const feedUrl = resolveVmgStockFeedUrl(dealerScope);
  if (!feedUrl) return false;
  if (vmgDealerHasDedicatedFeedUrl(dealerScope)) return true;
  if (!VMG_STOCK_FEED_URL) return false;
  const raw = String(dealerScope || "").trim().toLowerCase();
  if (!raw) return false;
  const digits = raw.replace(/\D+/g, "");
  const aliased = DEALER_ID_ALIASES?.get(raw) || "";
  const aliasedDigits = String(aliased).replace(/\D+/g, "");
  return Boolean(
    VMG_DEALER_SCOPES.has(raw) ||
    (digits && VMG_DEALER_SCOPES.has(digits)) ||
    (aliased && VMG_DEALER_SCOPES.has(String(aliased).toLowerCase())) ||
    (aliasedDigits && VMG_DEALER_SCOPES.has(aliasedDigits))
  );
}

function getVmgListingsBucket(feedUrl) {
  const key = String(feedUrl || "").trim();
  if (!key) return { rows: [], fetchedAtMs: 0 };
  let b = vmgListingsCacheByUrl.get(key);
  if (!b) {
    b = { rows: [], fetchedAtMs: 0 };
    vmgListingsCacheByUrl.set(key, b);
  }
  return b;
}

function vmgCachedRowsForDealerScope(dealerScope) {
  const url = resolveVmgStockFeedUrl(dealerScope);
  const b = getVmgListingsBucket(url);
  return Array.isArray(b.rows) ? b.rows : [];
}

function ensureDataDir() {
  if (!fs.existsSync(CONNECTOR_DATA_DIR)) {
    fs.mkdirSync(CONNECTOR_DATA_DIR, { recursive: true });
  }
}

function persistAutoTraderCache() {
  try {
    ensureDataDir();
    const payload = {
      savedAt: new Date().toISOString(),
      fetchedAtMs: autoTraderListingsCache.fetchedAtMs || 0,
      rows: Array.isArray(autoTraderListingsCache.rows) ? autoTraderListingsCache.rows : [],
    };
    fs.writeFileSync(AUTOTRADER_CACHE_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (e) {
    log("warn", "autotrader_cache_persist_failed", { error: e?.message || String(e) });
  }
}

function loadAutoTraderCache() {
  try {
    ensureDataDir();
    if (!fs.existsSync(AUTOTRADER_CACHE_FILE)) return;
    const raw = fs.readFileSync(AUTOTRADER_CACHE_FILE, "utf8");
    if (!raw.trim()) return;
    const parsed = JSON.parse(raw);
    const rows = Array.isArray(parsed?.rows) ? parsed.rows : [];
    if (rows.length === 0) return;
    autoTraderListingsCache.rows = rows;
    autoTraderListingsCache.fetchedAtMs = Number(parsed?.fetchedAtMs || Date.now());
    log("info", "autotrader_cache_loaded", {
      rows: rows.length,
      cachedAt: new Date(autoTraderListingsCache.fetchedAtMs).toISOString(),
    });
  } catch (e) {
    log("warn", "autotrader_cache_load_failed", { error: e?.message || String(e) });
  }
}

function persistVmgCache() {
  try {
    ensureDataDir();
    const feeds = {};
    for (const [url, bucket] of vmgListingsCacheByUrl) {
      feeds[url] = {
        fetchedAtMs: bucket.fetchedAtMs || 0,
        rows: Array.isArray(bucket.rows) ? bucket.rows : [],
      };
    }
    const payload = {
      version: 2,
      savedAt: new Date().toISOString(),
      feeds,
    };
    fs.writeFileSync(VMG_CACHE_FILE, JSON.stringify(payload, null, 2), "utf8");
  } catch (e) {
    log("warn", "vmg_cache_persist_failed", { error: e?.message || String(e) });
  }
}

function loadVmgCache() {
  try {
    ensureDataDir();
    if (!fs.existsSync(VMG_CACHE_FILE)) return;
    const raw = fs.readFileSync(VMG_CACHE_FILE, "utf8");
    if (!raw.trim()) return;
    const parsed = JSON.parse(raw);
    vmgListingsCacheByUrl.clear();
    if (parsed && parsed.version === 2 && parsed.feeds && typeof parsed.feeds === "object") {
      let total = 0;
      for (const [url, body] of Object.entries(parsed.feeds)) {
        if (!url) continue;
        const rows = Array.isArray(body?.rows) ? body.rows : [];
        vmgListingsCacheByUrl.set(url, {
          fetchedAtMs: Number(body?.fetchedAtMs || 0),
          rows,
        });
        total += rows.length;
      }
      log("info", "vmg_cache_loaded", {
        feeds: vmgListingsCacheByUrl.size,
        rows: total,
      });
      return;
    }
    const rows = Array.isArray(parsed?.rows) ? parsed.rows : [];
    if (rows.length === 0) return;
    const defaultUrl = (VMG_STOCK_FEED_URL || "").trim();
    if (defaultUrl) {
      vmgListingsCacheByUrl.set(defaultUrl, {
        fetchedAtMs: Number(parsed?.fetchedAtMs || Date.now()),
        rows,
      });
      log("info", "vmg_cache_loaded", {
        feeds: 1,
        rows: rows.length,
        legacy: true,
        cachedAt: new Date(Number(parsed?.fetchedAtMs || Date.now())).toISOString(),
      });
    }
  } catch (e) {
    log("warn", "vmg_cache_load_failed", { error: e?.message || String(e) });
  }
}

function decodeXmlText(value) {
  return String(value || "")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'");
}

function extractXmlTag(text, tagName) {
  const re = new RegExp(`<${tagName}>([\\s\\S]*?)</${tagName}>`, "i");
  const m = String(text || "").match(re);
  return m ? decodeXmlText(m[1]).trim() : "";
}

function extractXmlTagAll(text, tagName) {
  const out = [];
  const re = new RegExp(`<${tagName}>([\\s\\S]*?)</${tagName}>`, "gi");
  const body = String(text || "");
  let m = re.exec(body);
  while (m) {
    out.push(decodeXmlText(m[1]).trim());
    m = re.exec(body);
  }
  return out;
}

function parseVmgStockXml(xmlText) {
  const body = String(xmlText || "");
  const dealerName = extractXmlTag(body, "dealername");
  const dealerId = extractXmlTag(body, "dealerID");
  const branchName = extractXmlTag(body, "branchname");
  const vehicleBlocks = body.match(/<vehicle>[\s\S]*?<\/vehicle>/gi) || [];
  return vehicleBlocks.map((block) => {
    const variant = extractXmlTag(block, "variant");
    const imageUrls = extractXmlTagAll(block, "imgurl").filter((u) => u.startsWith("http://") || u.startsWith("https://"));
    const extras = extractXmlTag(block, "extras");
    const features = extras ? extras.split(",").map((s) => s.trim()).filter(Boolean) : [];
    const modelGuess = variant ? variant.split(/\s+/)[0] : "";
    return {
      stockNumber: extractXmlTag(block, "stockID"),
      registrationNumber: extractXmlTag(block, "licenceNumber"),
      make: extractXmlTag(block, "Make"),
      model: modelGuess,
      variant,
      newUsed: extractXmlTag(block, "newUsed"),
      year: Number(extractXmlTag(block, "year")) || null,
      mileageInKm: Number(extractXmlTag(block, "mileage")) || null,
      price: extractXmlTag(block, "price"),
      colour: extractXmlTag(block, "Colour"),
      bodyType: "",
      fuelType: "",
      transmission: "",
      transmissionDrive: "",
      dealerId: dealerId || null,
      vehicleCategory: "",
      vehicleSubCategory: "",
      description: extras || "",
      imageUrls,
      primaryImageUrl: pickFirstHttpImage(imageUrls),
      features,
      raw: {
        source: "vmg",
        dealerName,
        dealerID: dealerId,
        branchName,
        DateUpdated: extractXmlTag(block, "DateUpdated"),
        mmCode: extractXmlTag(block, "mmCode"),
        VIN: extractXmlTag(block, "VIN"),
        condition: extractXmlTag(block, "condition"),
        referenceID: extractXmlTag(block, "referenceID"),
        EstProfit: extractXmlTag(block, "EstProfit"),
      },
    };
  });
}

function persistStore() {
  ensureDataDir();
  const store = {
    version: 1,
    savedAt: new Date().toISOString(),
    commands: Array.from(commands.values()),
    idempotencyIndex: Object.fromEntries(idempotencyIndex.entries()),
    deadLetters,
    stockTakeSessions: Array.from(stockTakeSessions.values()),
    targetPlans: Array.from(targetPlans.values()),
    alertRules: Array.from(alertRules.values()),
    testDriveSessions: Array.from(testDriveSessions.values()),
    consentRecords: Array.from(consentRecords.values()),
    consentEvents: consentEvents.slice(-CONSENT_EVENTS_STORE_CAP),
    leadOwnerByLeadId: Object.fromEntries(leadOwnerByLeadId.entries()),
    leadOwnerByCorrelationId: Object.fromEntries(leadOwnerByCorrelationId.entries()),
  };
  fs.writeFileSync(STORE_FILE, JSON.stringify(store, null, 2), "utf8");
}

function loadStore() {
  ensureDataDir();
  if (!fs.existsSync(STORE_FILE)) return;
  const raw = fs.readFileSync(STORE_FILE, "utf8");
  if (!raw.trim()) return;
  const parsed = JSON.parse(raw);
  for (const record of parsed.commands || []) {
    if (record?.correlationId) {
      commands.set(record.correlationId, record);
    }
  }
  for (const [k, v] of Object.entries(parsed.idempotencyIndex || {})) {
    if (k && v) idempotencyIndex.set(k, v);
  }
  for (const d of parsed.deadLetters || []) {
    deadLetters.push(d);
  }
  for (const s of parsed.stockTakeSessions || []) {
    if (s?.sessionId) {
      stockTakeSessions.set(String(s.sessionId), s);
    }
  }
  for (const t of parsed.targetPlans || []) {
    const dealerId = String(t?.dealerId || "").trim();
    const period = String(t?.period || "").trim();
    if (!dealerId || !period) continue;
    const branchId = String(t?.branchId || "").trim();
    const key = `${dealerId}|${branchId}|${period}`;
    targetPlans.set(key, t);
  }
  for (const r of parsed.alertRules || []) {
    if (r?.ruleId) alertRules.set(String(r.ruleId), r);
  }
  for (const c of parsed.consentRecords || []) {
    if (c?.consentId) consentRecords.set(String(c.consentId), c);
  }
  for (const ev of parsed.consentEvents || []) {
    consentEvents.push(ev);
  }
  for (const td of parsed.testDriveSessions || []) {
    if (td?.sessionId) testDriveSessions.set(String(td.sessionId), td);
  }
  for (const [leadId, owner] of Object.entries(parsed.leadOwnerByLeadId || {})) {
    if (!leadId || !owner || typeof owner !== "object") continue;
    leadOwnerByLeadId.set(String(leadId), owner);
  }
  for (const [corr, owner] of Object.entries(parsed.leadOwnerByCorrelationId || {})) {
    if (!corr || !owner || typeof owner !== "object") continue;
    leadOwnerByCorrelationId.set(String(corr), owner);
  }
}

function normalizeCommandBody(body) {
  const commandTypeRaw = body?.commandType;
  const normalizedType = typeof commandTypeRaw === "string" ? commandTypeRaw.trim() : "";
  const commandType = LEGACY_VALUATION_COMMANDS.has(normalizedType) ? VALUATION_COMMAND : normalizedType;
  if (typeof commandType !== "string" || commandType.trim().length === 0) {
    return { error: "commandType is required" };
  }

  const correlationId = typeof body?.correlationId === "string" && body.correlationId.trim().length > 0
    ? body.correlationId.trim()
    : `scan_${uuidv4()}`;

  return {
    commandType: commandType.trim(),
    correlationId,
    payload: body?.payload ?? {},
    meta: body?.meta ?? {},
  };
}

function isPlainObject(value) {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function creditCheckMissingConsentHint(payload, tenantContext) {
  const leadCorr = String(payload?.leadCorrelationId || "").trim();
  const leadIdKey = String(payload?.leadId || "").trim();
  if (!leadCorr && !leadIdKey) return "";
  let pending = false;
  let approved = false;
  for (const c of consentRecords.values()) {
    if (tenantContext && !canAccessTenantScopedRecord(c.tenantContext || {}, tenantContext || {})) continue;
    const st = String(c.status || "").toLowerCase();
    const ccorr = String(c.leadCorrelationId || "").trim();
    const cid = String(c.leadId || "").trim();
    const matches =
      (leadCorr && ccorr && leadCorr === ccorr) || (leadIdKey && cid && leadIdKey === cid);
    if (!matches) continue;
    if (st === "pending") pending = true;
    if (st === "approved") approved = true;
  }
  if (pending) {
    return " For this lead, consent is still pending — wait for the customer to approve the email link, then retry (app must send payload.consentId).";
  }
  if (!pending && !approved) {
    return " No consent record for this lead yet — tap “Send approval link” in the app before running a credit check.";
  }
  if (approved) {
    return " An approved consent exists for this lead but consentId was not sent — update the mobile app or connector to the latest build.";
  }
  return "";
}

function validateCommandPayload(commandType, payload, tenantContext = null) {
  if (!isPlainObject(payload)) {
    return { error: "payload must be a JSON object" };
  }

  switch (commandType) {
    case "CREATE_LEAD": {
      const lead = payload.driverLicense || payload.lead || {};
      const hasName =
        String(lead.firstName || lead.NAMES || "").trim().length > 0 ||
        String(lead.lastName || lead.SURNAME || lead.surname || "").trim().length > 0;
      if (!hasName) {
        return {
          error: "invalid_payload",
          hint: "CREATE_LEAD payload requires payload.driverLicense or payload.lead with first/last name fields.",
        };
      }
      return null;
    }
    case "CREDIT_CHECK": {
      const consentId = String(payload?.consentId || "").trim();
      const inlineConsent = payload?.consent;
      const inlineAccepted =
        isPlainObject(inlineConsent) &&
        (inlineConsent.accepted === true || String(inlineConsent.accepted || "").toLowerCase() === "true");
      if (!consentId && !inlineAccepted) {
        const extra = creditCheckMissingConsentHint(payload, tenantContext);
        return {
          error: "invalid_payload",
          hint:
            "CREDIT_CHECK requires payload.consentId (email approval) or payload.consent.accepted=true (in-app checkbox)." +
            extra,
        };
      }
      const applicant = payload.applicant || {};
      const idNumber = String(applicant.idNumber || payload.idNumber || "").trim();
      const mobile = String(applicant.mobile || applicant.contact || payload.phone || "").trim();
      if (!idNumber || !mobile) {
        return {
          error: "invalid_payload",
          hint: "CREDIT_CHECK requires applicant idNumber and mobile/contact.",
        };
      }
      if (!payload.leadId && !payload.leadCorrelationId) {
        return {
          error: "invalid_payload",
          hint: "CREDIT_CHECK requires leadId or leadCorrelationId (create lead first).",
        };
      }
      return null;
    }
    case "SHARE_LEAD": {
      if (!payload.leadId && !payload.leadCorrelationId) {
        return { error: "invalid_payload", hint: "SHARE_LEAD requires leadId or leadCorrelationId." };
      }
      return null;
    }
    case "LOG_COMMUNICATION": {
      if (!payload.leadId && !payload.leadCorrelationId) {
        return { error: "invalid_payload", hint: "LOG_COMMUNICATION requires leadId or leadCorrelationId." };
      }
      const content = String(payload?.comment?.content || payload?.content || "").trim();
      if (!content) {
        return { error: "invalid_payload", hint: "LOG_COMMUNICATION requires comment content." };
      }
      return null;
    }
    case "CREATE_STOCK_UNIT": {
      const vehicle = payload.vehicle || payload.stock || {};
      const hasVehicleIdentity =
        String(vehicle.make || "").trim().length > 0 ||
        String(vehicle.model || "").trim().length > 0 ||
        String(vehicle.registration || vehicle.licenceNumber || "").trim().length > 0;
      if (!hasVehicleIdentity) {
        return {
          error: "invalid_payload",
          hint: "CREATE_STOCK_UNIT payload requires vehicle/stock data (make/model or registration/licenceNumber).",
        };
      }
      return null;
    }
    case "SEND_STOCK_TO_LEAD": {
      const hasLead = Boolean(payload.leadId || payload.leadCorrelationId);
      const hasStock = Boolean(payload.stockUnitId || payload.stockCorrelationId);
      if (!hasLead || !hasStock) {
        return {
          error: "invalid_payload",
          hint: "SEND_STOCK_TO_LEAD requires leadId/leadCorrelationId and stockUnitId/stockCorrelationId.",
        };
      }
      return null;
    }
    case "TRADE_IN": {
      const requiredViews = ["front", "left", "right", "back"];
      const photos = isPlainObject(payload.photos) ? payload.photos : {};
      const missingViews = requiredViews.filter((view) => String(photos[view] || "").trim().length === 0);
      if (missingViews.length > 0) {
        return {
          error: "invalid_payload",
          hint: `TRADE_IN requires photo URLs for views: ${requiredViews.join(", ")}. Missing: ${missingViews.join(", ")}.`,
        };
      }

      const wireframe = isPlainObject(payload.damageWireframe) ? payload.damageWireframe : {};
      const damages = Array.isArray(payload.damages) ? payload.damages : [];
      if (!wireframe || !Array.isArray(wireframe.markers)) {
        return {
          error: "invalid_payload",
          hint: "TRADE_IN requires payload.damageWireframe.markers (array) for damage markup points.",
        };
      }
      if (damages.length === 0) {
        return {
          error: "invalid_payload",
          hint: "TRADE_IN requires payload.damages with at least one damage item and reconCost.",
        };
      }
      const invalidDamage = damages.find((item) => {
        const recon = Number(item?.reconCost);
        return !String(item?.zone || "").trim() || !String(item?.description || "").trim() || !Number.isFinite(recon) || recon < 0;
      });
      if (invalidDamage) {
        return {
          error: "invalid_payload",
          hint: "Each TRADE_IN damage item must include zone, description, and non-negative reconCost.",
        };
      }
      return null;
    }
    case "STOCK_TAKE":
      return null;
    case VALUATION_COMMAND: {
      const required = ["make", "model", "variant", "year", "mileage"];
      const missing = required.filter((k) => String(payload[k] || "").trim().length === 0);
      if (missing.length > 0) {
        return {
          error: "invalid_payload",
          hint: `${VALUATION_COMMAND} requires: ${required.join(", ")}. Missing: ${missing.join(", ")}.`,
        };
      }
      return null;
    }
    default:
      return { error: "unsupported_command_type" };
  }
}

function sendApiError(req, res, status, error, hint = null, extra = {}) {
  if (status >= 400) {
    log(status >= 500 ? "error" : "warn", "api_rejected", {
      requestId: req.requestId || null,
      status,
      error,
      hint,
      path: req.path || null,
      method: req.method || null,
      tenantUserEmail: req.tenantContext?.userEmail || null,
      tenantDealerId: req.tenantContext?.dealerId || null,
      tenantRole: req.tenantContext?.role || null,
    });
  }
  return res.status(status).json({
    error,
    hint,
    requestId: req.requestId || null,
    ...extra,
  });
}

function buildTenantScope(tenantContext = {}) {
  return {
    dealerId: String(tenantContext?.dealerId || "").trim(),
    branchId: String(tenantContext?.branchId || "").trim(),
    userId: tenantContext?.userId || null,
  };
}

function canAccessTenantScopedRecord(recordTenant = {}, requestTenant = {}) {
  const recordDealer = String(recordTenant?.dealerId || "").trim();
  const requestDealer = String(requestTenant?.dealerId || "").trim();
  if (!recordDealer || !requestDealer || recordDealer !== requestDealer) return false;
  const recordBranch = String(recordTenant?.branchId || "").trim();
  const requestBranch = String(requestTenant?.branchId || "").trim();
  if (!recordBranch || !requestBranch) return true;
  return recordBranch === requestBranch;
}

function sanitizeConsentDeliveryForApi(delivery) {
  if (!delivery || typeof delivery !== "object") return delivery || null;
  const { approveUrl: _omit, ...rest } = delivery;
  return rest;
}

/** Summary for list/create responses — omits magic-link URL (token) from API JSON. */
function consentPublicView(record) {
  return {
    consentId: record.consentId,
    status: record.status,
    purpose: record.purpose,
    noticeVersion: record.noticeVersion,
    requestedAt: record.requestedAt,
    expiresAt: record.expiresAt,
    approvedAt: record.approvedAt || null,
    rejectedAt: record.rejectedAt || null,
    revokedAt: record.revokedAt || null,
    leadCorrelationId: record.leadCorrelationId || null,
    leadId: record.leadId || null,
    approvalChannel: record.approvalChannel || null,
    delivery: sanitizeConsentDeliveryForApi(record.delivery),
  };
}

/**
 * POPIA / audit trail: who requested, from where, customer-facing identifiers,
 * approval/reject context from the public link, append-only events, downstream use.
 */
function consentAuditPayload(record) {
  const consentId = String(record?.consentId || "").trim();
  const events = consentEvents.filter((e) => e && String(e.consentId || "") === consentId);
  return {
    requestedBy: record.requestedBy || null,
    requestMeta: record.requestMeta || null,
    applicant: record.applicant || null,
    approvalMeta: record.approvalMeta || null,
    revokeReason: record.revokeReason || null,
    usedAt: record.usedAt || null,
    creditCheckCorrelationId: record.creditCheckCorrelationId || null,
    tokenIssuedAt: record.tokenIssuedAt || null,
    updatedAt: record.updatedAt || null,
    events,
  };
}

function appendConsentEvent(consentId, type, extra = {}) {
  consentEvents.push({
    eventId: `cev_${uuidv4()}`,
    consentId,
    type,
    at: new Date().toISOString(),
    ...extra,
  });
}

function normalizeEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function maskIdNumber(idNumber) {
  const digits = String(idNumber || "").replace(/\D+/g, "");
  if (digits.length < 6) return "";
  return `${digits.slice(0, 6)}******${digits.slice(-2)}`;
}

function base64UrlEncode(input) {
  return Buffer.from(input)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function base64UrlDecode(input) {
  const normalized = String(input || "").replace(/-/g, "+").replace(/_/g, "/");
  const padded = normalized + "=".repeat((4 - (normalized.length % 4)) % 4);
  return Buffer.from(padded, "base64").toString("utf8");
}

function signConsentToken(payload) {
  const payloadText = JSON.stringify(payload);
  const encodedPayload = base64UrlEncode(payloadText);
  const signature = crypto
    .createHmac("sha256", CONSENT_TOKEN_SECRET)
    .update(encodedPayload)
    .digest("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
  return `${encodedPayload}.${signature}`;
}

function verifyConsentToken(token) {
  const raw = String(token || "").trim();
  if (!raw || !raw.includes(".")) throw new Error("invalid_token");
  const [payloadEnc, providedSig] = raw.split(".");
  const expectedSig = crypto
    .createHmac("sha256", CONSENT_TOKEN_SECRET)
    .update(payloadEnc)
    .digest("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
  const match =
    providedSig &&
    expectedSig.length === providedSig.length &&
    crypto.timingSafeEqual(Buffer.from(expectedSig), Buffer.from(providedSig));
  if (!match) throw new Error("invalid_token_signature");
  const payload = JSON.parse(base64UrlDecode(payloadEnc));
  const expMs = Number(payload?.exp || 0) * 1000;
  if (!Number.isFinite(expMs) || Date.now() >= expMs) throw new Error("token_expired");
  return payload;
}

function resolveConsentLinkBase(req) {
  if (CONSENT_LINK_BASE_URL) return CONSENT_LINK_BASE_URL.replace(/\/$/, "");
  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "http").split(",")[0].trim();
  const host = String(req.headers["x-forwarded-host"] || req.headers.host || "").split(",")[0].trim();
  return `${proto}://${host}`.replace(/\/$/, "");
}

function buildConsentApprovalLink(req, token) {
  const base = resolveConsentLinkBase(req);
  return `${base}/consent/approve?token=${encodeURIComponent(token)}`;
}

function parseDealerEntityLabel(value) {
  const v = String(value || "").trim();
  if (!v) return "";
  const sep = v.indexOf("|");
  if (sep <= 0) return v;
  return v.slice(sep + 1).trim() || v.slice(0, sep).trim();
}

function resolveDealerDisplayName(tenantContext = {}) {
  const dealerId = String(tenantContext?.dealerId || "").trim();
  if (dealerId && EVOLVESA_LEAD_RECEIVING_ENTITY_MAP.has(dealerId)) {
    const label = parseDealerEntityLabel(EVOLVESA_LEAD_RECEIVING_ENTITY_MAP.get(dealerId));
    if (label) return label;
  }
  return dealerId ? `Dealer ${dealerId}` : "Your dealership";
}

function smtpConfigured() {
  return Boolean(SMTP_HOST && SMTP_PORT && SMTP_USER && SMTP_PASS);
}

function brevoApiConfigured() {
  return Boolean(BREVO_API_KEY);
}

/** Consent email: Brevo HTTPS API and/or classic SMTP (SMTP blocked on Render free web services). */
function consentEmailDispatchConfigured() {
  return brevoApiConfigured() || smtpConfigured();
}

let consentMailer = null;
function getConsentMailer() {
  if (!smtpConfigured()) return null;
  if (consentMailer) return consentMailer;
  // Lazy-load to avoid hard dependency in deployments not using email yet.
  const nodemailer = require("nodemailer");
  consentMailer = nodemailer.createTransport({
    host: SMTP_HOST,
    port: SMTP_PORT,
    secure: SMTP_SECURE,
    auth: {
      user: SMTP_USER,
      pass: SMTP_PASS,
    },
    // Avoid hanging until the host/proxy times out (no email + no clear error in logs).
    connectionTimeout: 25_000,
    greetingTimeout: 20_000,
    socketTimeout: 45_000,
    tls: { minVersion: "TLSv1.2" },
  });
  return consentMailer;
}

function resolveConsentFromAddress(tenantContext = {}, dealerDisplayName = "Dealership") {
  const dealerId = String(tenantContext?.dealerId || "").trim();
  const mappedFrom = dealerId ? String(CONSENT_FROM_EMAIL_BY_DEALER.get(dealerId) || "").trim() : "";
  const fromEmail = mappedFrom || SMTP_FROM_EMAIL || SMTP_USER;
  return `"${dealerDisplayName}" <${fromEmail}>`;
}

function resolveConsentSenderParts(tenantContext = {}, dealerDisplayName = "Dealership") {
  const dealerId = String(tenantContext?.dealerId || "").trim();
  const mappedFrom = dealerId ? String(CONSENT_FROM_EMAIL_BY_DEALER.get(dealerId) || "").trim() : "";
  const fromEmail = (mappedFrom || SMTP_FROM_EMAIL || SMTP_USER || "").trim();
  return { name: dealerDisplayName, email: fromEmail };
}

async function sendConsentEmailViaBrevoApi(
  consentRecord,
  recipient,
  subject,
  text,
  html,
  providerRef,
  dealerDisplayName
) {
  const { name, email: fromEmail } = resolveConsentSenderParts(consentRecord.tenantContext || {}, dealerDisplayName);
  if (!fromEmail) {
    return { emailSent: false, providerRef, warning: "brevo_missing_from_email" };
  }
  try {
    const res = await fetch("https://api.brevo.com/v3/smtp/email", {
      method: "POST",
      headers: {
        accept: "application/json",
        "content-type": "application/json",
        "api-key": BREVO_API_KEY,
      },
      body: JSON.stringify({
        sender: { name, email: fromEmail },
        to: [{ email: recipient }],
        subject,
        textContent: text,
        htmlContent: html,
      }),
    });
    const raw = await res.text();
    if (!res.ok) {
      log("error", "consent_email_brevo_api_failed", {
        consentId: consentRecord.consentId,
        to: recipient,
        httpStatus: res.status,
        body: raw.slice(0, 500),
      });
      return {
        emailSent: false,
        providerRef,
        warning: `brevo_api_${res.status}: ${raw.slice(0, 180)}`,
      };
    }
    let providerMessageId = providerRef;
    try {
      const jo = JSON.parse(raw);
      if (jo.messageId) providerMessageId = String(jo.messageId);
    } catch (_) {
      /* ignore */
    }
    log("info", "consent_email_sent", {
      consentId: consentRecord.consentId,
      to: recipient,
      dealerId: consentRecord.tenantContext?.dealerId || null,
      providerRef: providerMessageId,
      via: "brevo_api",
    });
    return { emailSent: true, providerRef: providerMessageId };
  } catch (e) {
    log("error", "consent_email_brevo_api_failed", {
      consentId: consentRecord.consentId,
      to: recipient,
      error: e?.message || String(e),
    });
    return { emailSent: false, providerRef, warning: e?.message || String(e) };
  }
}

async function notifyConsentEmail(consentRecord, approveUrl) {
  const providerRef = `email_${uuidv4()}`;
  const recipient = normalizeEmail(consentRecord.applicant?.email || "");
  if (!recipient) {
    return { emailSent: false, providerRef, warning: "missing_recipient_email" };
  }
  const dealerDisplayName = resolveDealerDisplayName(consentRecord.tenantContext || {});
  if (!consentEmailDispatchConfigured()) {
    log("warn", "consent_email_not_sent_smtp_not_configured", {
      consentId: consentRecord.consentId,
      to: recipient,
      dealerId: consentRecord.tenantContext?.dealerId || null,
      hint: "Set BREVO_API_KEY (HTTPS) and/or SMTP_* on paid Render or local.",
    });
    return { emailSent: false, providerRef, warning: "smtp_not_configured" };
  }

  const consentExpiry = consentRecord.expiresAt || "";
  const customerName = [consentRecord.applicant?.firstName, consentRecord.applicant?.surname].filter(Boolean).join(" ").trim();
  const subject = `${dealerDisplayName}: Approve your soft credit check`;
  const text =
    `Hi ${customerName || "Customer"},\n\n` +
    `${dealerDisplayName} requests your approval to run a soft credit check.\n` +
    `Purpose: ${consentRecord.purpose}\n` +
    `Reference: ${consentRecord.consentId}\n` +
    `ID: ${consentRecord.applicant?.idNumberMasked || ""}\n` +
    `Valid until: ${consentExpiry}\n\n` +
    `Approve or reject here:\n${approveUrl}\n\n` +
    `If you did not request this, please ignore this email and contact the dealership.`;
  const html =
    `<p>Hi ${customerName || "Customer"},</p>` +
    `<p><strong>${dealerDisplayName}</strong> requests your approval to run a soft credit check.</p>` +
    `<p>Purpose: ${consentRecord.purpose}<br/>Reference: ${consentRecord.consentId}<br/>` +
    `ID: ${consentRecord.applicant?.idNumberMasked || ""}<br/>Valid until: ${consentExpiry}</p>` +
    `<p><a href="${approveUrl}">Review and approve/reject consent</a></p>` +
    `<p>If you did not request this, please ignore this email and contact the dealership.</p>`;

  if (brevoApiConfigured()) {
    const toDomain = recipient.includes("@") ? recipient.split("@").pop() : "";
    log("info", "consent_email_send_start", {
      consentId: consentRecord.consentId,
      toDomain,
      via: "brevo_api",
      dealerId: consentRecord.tenantContext?.dealerId || null,
    });
    return await sendConsentEmailViaBrevoApi(
      consentRecord,
      recipient,
      subject,
      text,
      html,
      providerRef,
      dealerDisplayName
    );
  }

  const transporter = getConsentMailer();
  try {
    const toDomain = recipient.includes("@") ? recipient.split("@").pop() : "";
    log("info", "consent_email_send_start", {
      consentId: consentRecord.consentId,
      toDomain,
      smtpHost: SMTP_HOST,
      smtpPort: SMTP_PORT,
      dealerId: consentRecord.tenantContext?.dealerId || null,
      via: "smtp",
    });
    const mailOptions = {
      from: resolveConsentFromAddress(consentRecord.tenantContext || {}, dealerDisplayName),
      to: recipient,
      subject,
      text,
      html,
    };
    const smtpHardTimeoutMs = 50_000;
    const info = await Promise.race([
      transporter.sendMail(mailOptions),
      new Promise((_, reject) => {
        setTimeout(() => {
          const err = new Error(
            `SMTP send exceeded ${smtpHardTimeoutMs}ms (outbound SMTP is often blocked on Render free tier; set BREVO_API_KEY)`
          );
          err.code = "ESMTP_HARD_TIMEOUT";
          reject(err);
        }, smtpHardTimeoutMs);
      }),
    ]);
    log("info", "consent_email_sent", {
      consentId: consentRecord.consentId,
      to: recipient,
      dealerId: consentRecord.tenantContext?.dealerId || null,
      providerRef: info?.messageId || providerRef,
      via: "smtp",
    });
    return {
      emailSent: true,
      providerRef: info?.messageId || providerRef,
    };
  } catch (e) {
    log("error", "consent_email_send_failed", {
      consentId: consentRecord.consentId,
      to: recipient,
      error: e?.message || String(e),
      code: e?.code || null,
      command: e?.command || null,
      response: e?.response || null,
      via: "smtp",
    });
    return {
      emailSent: false,
      providerRef,
      warning: e?.message || String(e),
    };
  }
}

async function sendConsentApprovalFollowupEmail(consentRecord) {
  if (!CONSENT_APPROVAL_FOLLOWUP_EMAIL_ENABLED) {
    return { emailSent: false, providerRef: null, warning: "followup_disabled" };
  }
  const recipient = normalizeEmail(consentRecord?.applicant?.email || "");
  if (!recipient) {
    return { emailSent: false, providerRef: null, warning: "missing_recipient_email" };
  }
  const dealerDisplayName = resolveDealerDisplayName(consentRecord?.tenantContext || {});
  const customerName = [consentRecord?.applicant?.firstName, consentRecord?.applicant?.surname].filter(Boolean).join(" ").trim();
  const lines = [];
  if (CONSENT_POPIA_NOTICE_URL) lines.push(`POPIA consent notice: ${CONSENT_POPIA_NOTICE_URL}`);
  if (CONSENT_POPIA_PRIVACY_URL) lines.push(`Privacy policy: ${CONSENT_POPIA_PRIVACY_URL}`);
  if (CONSENT_POPIA_TERMS_URL) lines.push(`Terms of processing: ${CONSENT_POPIA_TERMS_URL}`);
  const docsText = lines.length > 0 ? `\n\nDocuments:\n${lines.join("\n")}` : "";
  const docsHtml = [
    CONSENT_POPIA_NOTICE_URL ? `<li><a href="${CONSENT_POPIA_NOTICE_URL}">POPIA consent notice</a></li>` : "",
    CONSENT_POPIA_PRIVACY_URL ? `<li><a href="${CONSENT_POPIA_PRIVACY_URL}">Privacy policy</a></li>` : "",
    CONSENT_POPIA_TERMS_URL ? `<li><a href="${CONSENT_POPIA_TERMS_URL}">Terms of processing</a></li>` : "",
  ].filter(Boolean).join("");
  const subject = `${dealerDisplayName}: Thank you for approving your soft credit check`;
  const text =
    `Hi ${customerName || "Customer"},\n\n` +
    `Thank you for approving the soft credit check request.\n` +
    `Reference: ${consentRecord?.consentId || ""}\n` +
    `Approved at: ${consentRecord?.approvedAt || new Date().toISOString()}` +
    docsText +
    `\n\nIf you need assistance, please contact ${dealerDisplayName}.`;
  const html =
    `<p>Hi ${customerName || "Customer"},</p>` +
    `<p>Thank you for approving the soft credit-check request.</p>` +
    `<p>Reference: <strong>${consentRecord?.consentId || ""}</strong><br/>Approved at: ${consentRecord?.approvedAt || new Date().toISOString()}</p>` +
    (docsHtml ? `<p>Please review the following POPIA-related documents:</p><ul>${docsHtml}</ul>` : "") +
    `<p>If you need assistance, please contact ${dealerDisplayName}.</p>`;

  const providerRef = `email_followup_${uuidv4()}`;
  if (brevoApiConfigured()) {
    return await sendConsentEmailViaBrevoApi(
      consentRecord,
      recipient,
      subject,
      text,
      html,
      providerRef,
      dealerDisplayName
    );
  }
  const transporter = getConsentMailer();
  if (!transporter) {
    return { emailSent: false, providerRef, warning: "smtp_not_configured" };
  }
  try {
    const info = await transporter.sendMail({
      from: resolveConsentFromAddress(consentRecord?.tenantContext || {}, dealerDisplayName),
      to: recipient,
      subject,
      text,
      html,
    });
    return { emailSent: true, providerRef: info?.messageId || providerRef };
  } catch (e) {
    return { emailSent: false, providerRef, warning: e?.message || String(e) };
  }
}

/**
 * If the app omitted consentId after a refresh, locate the dealer-scoped APPROVED consent for this lead.
 */
function inferApprovedConsentIdForCreditCheck(payload, tenantContext) {
  if (!isPlainObject(payload)) return null;
  if (String(payload.consentId || "").trim()) return null;
  const inlineConsent = payload.consent;
  const inlineAccepted =
    isPlainObject(inlineConsent) &&
    (inlineConsent.accepted === true || String(inlineConsent.accepted || "").toLowerCase() === "true");
  if (inlineAccepted) return null;

  const leadCorr = String(payload.leadCorrelationId || "").trim();
  const leadId = String(payload.leadId || "").trim();
  if (!leadCorr && !leadId) return null;

  const candidates = [];
  for (const consent of consentRecords.values()) {
    if (String(consent.status || "").toLowerCase() !== "approved") continue;
    if (!canAccessTenantScopedRecord(consent.tenantContext || {}, tenantContext || {})) continue;
    if (consent.expiresAt && Date.parse(consent.expiresAt) <= Date.now()) continue;
    if (consent.revokedAt) continue;

    const ccorr = String(consent.leadCorrelationId || "").trim();
    const cid = String(consent.leadId || "").trim();

    let match = false;
    if (leadCorr && ccorr && leadCorr === ccorr) match = true;
    else if (leadId && cid && leadId === cid) match = true;
    if (!match) continue;

    candidates.push({
      consentId: String(consent.consentId || "").trim(),
      t: consent.approvedAt ? Date.parse(consent.approvedAt) || 0 : 0,
    });
  }
  if (candidates.length === 0) return null;
  candidates.sort((a, b) => b.t - a.t);
  return candidates[0].consentId || null;
}

function resolveApprovedConsent(payload, tenantContext) {
  const consentId = String(payload?.consentId || "").trim();
  if (!consentId) {
    throw new Error("consentId is required before soft credit check");
  }
  const consent = consentRecords.get(consentId);
  if (!consent) {
    throw new Error("Consent record not found");
  }
  if (!canAccessTenantScopedRecord(consent.tenantContext || {}, tenantContext || {})) {
    throw new Error("Consent does not belong to current tenant scope");
  }
  if (String(consent.status || "").toLowerCase() !== "approved") {
    throw new Error("Consent is not approved");
  }
  if (consent.expiresAt && Date.parse(consent.expiresAt) <= Date.now()) {
    throw new Error("Consent has expired");
  }
  if (consent.revokedAt) {
    throw new Error("Consent has been revoked");
  }
  const payloadCorr = String(payload?.leadCorrelationId || "").trim();
  const payloadId = String(payload?.leadId || "").trim();
  const consentCorr = String(consent.leadCorrelationId || "").trim();
  const consentLeadIdField = String(consent.leadId || "").trim();
  const anyPayloadLead = Boolean(payloadCorr || payloadId);
  const anyConsentLead = Boolean(consentCorr || consentLeadIdField);
  if (anyPayloadLead && anyConsentLead) {
    const corrMatch = Boolean(payloadCorr && consentCorr && payloadCorr === consentCorr);
    const idMatch = Boolean(payloadId && consentLeadIdField && payloadId === consentLeadIdField);
    if (!corrMatch && !idMatch) {
      throw new Error("Consent does not match the requested lead");
    }
  }
  return consent;
}

function toIsoDate(value) {
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function monthPeriod(value = new Date()) {
  const d = new Date(value);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function tenantKey(dealerId, branchId = "", period = monthPeriod()) {
  return `${String(dealerId || "").trim()}|${String(branchId || "").trim()}|${String(period || "").trim()}`;
}

function computeBandLabel(value, target) {
  const v = Number(value || 0);
  const t = Number(target || 0);
  if (!Number.isFinite(t) || t <= 0) return "no_target";
  const ratio = v / t;
  if (ratio >= 1) return "on_track";
  if (ratio >= 0.8) return "at_risk";
  return "off_track";
}

function commandsInWindow(fromIso, toIso) {
  const fromMs = fromIso ? Date.parse(fromIso) : Number.NEGATIVE_INFINITY;
  const toMs = toIso ? Date.parse(toIso) : Number.POSITIVE_INFINITY;
  const rows = [];
  for (const c of commands.values()) {
    const ts = Date.parse(c?.createdAt || 0);
    if (!Number.isFinite(ts)) continue;
    if (ts < fromMs || ts > toMs) continue;
    rows.push(c);
  }
  return rows;
}

function aggregateKpis(rows = [], dealerId = "", branchId = "") {
  const matchesTenant = (c) => {
    const d = String(c?.tenantContext?.dealerId || "").trim();
    const b = String(c?.tenantContext?.branchId || "").trim();
    if (dealerId && d !== dealerId) return false;
    if (branchId && b !== branchId) return false;
    return true;
  };
  const inTenant = rows.filter(matchesTenant);
  const done = inTenant.filter((c) => c.status === "done");
  const failed = inTenant.filter((c) => c.status === "failed");
  const createdLeads = done.filter((c) => c.commandType === "CREATE_LEAD").length;
  const creditChecks = done.filter((c) => c.commandType === "CREDIT_CHECK").length;
  const stockUnits = done.filter((c) => c.commandType === "CREATE_STOCK_UNIT").length;
  const sharedStock = done.filter((c) => c.commandType === "SEND_STOCK_TO_LEAD").length;
  const approvalsPending = inTenant.filter((c) => c.status === "pending_manager_approval").length;
  const avgProcessMs = (() => {
    const samples = done
      .map((c) => {
        const a = Date.parse(c?.createdAt || 0);
        const b = Date.parse(c?.updatedAt || 0);
        if (!Number.isFinite(a) || !Number.isFinite(b) || b < a) return null;
        return b - a;
      })
      .filter((x) => Number.isFinite(x));
    if (samples.length === 0) return null;
    return Math.round(samples.reduce((acc, x) => acc + x, 0) / samples.length);
  })();
  return {
    totalCommands: inTenant.length,
    done: done.length,
    failed: failed.length,
    createdLeads,
    creditChecks,
    stockUnits,
    sharedStock,
    approvalsPending,
    avgProcessMs,
    leadToStockUnitRate: createdLeads > 0 ? Number((stockUnits / createdLeads).toFixed(3)) : null,
    leadToShareRate: createdLeads > 0 ? Number((sharedStock / createdLeads).toFixed(3)) : null,
  };
}

function rollingForecast(currentValue, elapsedDays, windowDays = 30) {
  const v = Number(currentValue || 0);
  const e = Number(elapsedDays || 0);
  if (!Number.isFinite(v) || !Number.isFinite(e) || e <= 0) return null;
  return Math.round((v / e) * windowDays);
}

function buildStockTakeItemFromStock(stock, source, extra = {}) {
  return {
    itemId: `stkitem_${uuidv4()}`,
    source,
    status: source === "autotrader_list" ? "matched" : "new_unmatched",
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    stock: stock || null,
    scan: extra.scan || null,
    manual: extra.manual || null,
    linkedStock: source === "autotrader_list" ? (stock || null) : null,
    pricing: {
      autoTraderPrice: stock?.autoTraderPrice ?? stock?.price ?? null,
      tradePrice: stock?.tradePrice ?? null,
      retailPrice: stock?.retailPrice ?? null,
      marketPrice: stock?.marketPrice ?? null,
      valuationStatus: stock?.valuationStatus || null,
    },
    notes: String(extra.notes || "").trim() || null,
  };
}

function ensureStockTakeSession(sessionId, tenantContext) {
  const existing = stockTakeSessions.get(sessionId);
  if (existing) return existing;
  const session = {
    sessionId,
    tenantContext: buildTenantScope(tenantContext),
    status: "open",
    items: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    submittedAt: null,
    submittedSummary: null,
  };
  stockTakeSessions.set(sessionId, session);
  persistStore();
  return session;
}

function createOrUpdateCommand(record) {
  commands.set(record.correlationId, record);
  if (record.idempotencyKey && record.commandType === "CREATE_LEAD") {
    idempotencyIndex.set(record.idempotencyKey, record.correlationId);
  }
  persistStore();
}

function resolveLeadOwner(payload = {}) {
  const leadId = String(payload?.leadId || "").trim();
  if (leadId && leadOwnerByLeadId.has(leadId)) {
    return leadOwnerByLeadId.get(leadId);
  }
  const corr = String(payload?.leadCorrelationId || "").trim();
  if (corr && leadOwnerByCorrelationId.has(corr)) {
    return leadOwnerByCorrelationId.get(corr);
  }
  if (corr && commands.has(corr)) {
    const c = commands.get(corr);
    const resolvedLeadId = String(c?.result?.leadId || "").trim();
    if (resolvedLeadId && leadOwnerByLeadId.has(resolvedLeadId)) {
      return leadOwnerByLeadId.get(resolvedLeadId);
    }
  }
  return null;
}

function assignLeadOwnerIndex(correlationId, leadId, tenantContext = {}) {
  const owner = {
    ownerUserId: tenantContext?.userId || null,
    ownerEmail: String(tenantContext?.userEmail || "").trim().toLowerCase() || null,
    ownerName: tenantContext?.userName || null,
    ownerRole: normalizeRole(tenantContext?.role),
    dealerId: String(tenantContext?.dealerId || "").trim() || null,
    branchId: String(tenantContext?.branchId || "").trim() || null,
    assignedAt: new Date().toISOString(),
  };
  const corr = String(correlationId || "").trim();
  const lid = String(leadId || "").trim();
  if (corr) leadOwnerByCorrelationId.set(corr, owner);
  if (lid) leadOwnerByLeadId.set(lid, owner);
}

function enforceLeadOwnershipForCommand(commandType, payload = {}, tenantContext = {}) {
  if (!LEAD_OWNERSHIP_ENFORCED_COMMANDS.has(commandType)) return null;
  const role = normalizeRole(tenantContext?.role);
  if (role !== "sales_person") return null;
  const owner = resolveLeadOwner(payload);
  if (!owner) return null;
  const actorUserId = tenantContext?.userId || null;
  const actorEmail = String(tenantContext?.userEmail || "").trim().toLowerCase();
  const ownerUserId = owner?.ownerUserId || null;
  const ownerEmail = String(owner?.ownerEmail || "").trim().toLowerCase();
  const sameUserId = Boolean(actorUserId && ownerUserId && actorUserId === ownerUserId);
  const sameEmail = Boolean(actorEmail && ownerEmail && actorEmail === ownerEmail);
  if (sameUserId || sameEmail) return null;
  const label = owner?.ownerName || ownerEmail || ownerUserId || "another consultant";
  return {
    error: "lead_ownership_forbidden",
    hint: `This lead is owned by ${label}. Ask a sales manager or dealer principal to reassign it.`,
  };
}

function pushDeadLetter(record, errorMessage) {
  deadLetters.push({
    deadLetterId: `dlq_${uuidv4()}`,
    failedAt: new Date().toISOString(),
    correlationId: record.correlationId,
    commandType: record.commandType,
    payload: record.payload,
    attempts: record.attempts || 0,
    error: errorMessage,
  });
  persistStore();
}

function evolveConfigured() {
  return Boolean(EVOLVESA_BASE_URL && EVOLVESA_API_KEY);
}

function toEvolveLeadPayload(payload) {
  const dl = payload?.driverLicense || payload?.lead || {};
  const tenant = payload?._tenantContext || {};
  const normalize = (v) =>
    String(v || "")
      .replace(/\s+/g, " ")
      .trim();
  const combinedName = normalize(dl.name || payload?.name || "");
  let firstName = normalize(dl.NAMES || dl.firstName || payload?.firstName || "");
  let lastName = normalize(dl.SURNAME || dl.lastName || dl.surname || payload?.lastName || payload?.surname || "");
  if ((!firstName || !lastName) && combinedName) {
    const parts = combinedName.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      if (!lastName) lastName = parts[parts.length - 1];
      if (!firstName) firstName = parts.slice(0, -1).join(" ");
    } else if (!firstName) {
      firstName = parts[0] || "";
    }
  }
  return {
    firstName,
    lastName,
    idNumber: dl.ID_NUMBER || dl.idNumber || "",
    licenseNumber: dl.LICENSE_NUMBER || dl.licenseNumber || "",
    phone: dl.phone || dl.mobile || payload?.phone || "",
    email: dl.email || payload?.email || "",
    dealerId: tenant.dealerId || "",
    branchId: tenant.branchId || "",
    createdByUserId: tenant.userId || "",
    createdByName: tenant.userName || "",
    createdByEmail: tenant.userEmail || "",
    createdByRole: tenant.role || "",
    source: payload?.source || "CubeOneScan",
    raw: payload,
  };
}

function toEvolveStockPayload(payload) {
  const stock = payload?.vehicle || payload?.stock || {};
  const tenant = payload?._tenantContext || {};
  const resolvedSource = payload?.source || EVOLVESA_STOCK_SOURCE;
  return {
    year: stock.year || stock.firstRegistrationYear || "",
    make: stock.make || "",
    model: stock.model || "",
    variant: stock.variant || "",
    mileage: stock.mileage || "",
    price: stock.price || "",
    colour: stock.color || stock.colour || "",
    vinNumber: stock.vin || "",
    licensePlate: stock.registration || stock.licenceNumber || "",
    category: stock.category || "",
    notes: stock.notes || "",
    engineNumber: stock.engineNumber || "",
    expiryDate: stock.expiry || "",
    firstRegistrationDate: stock.firstRegistrationDate || "",
    firstRegistrationYear: stock.firstRegistrationYear || "",
    dealerId: tenant.dealerId || "",
    branchId: tenant.branchId || "",
    createdByUserId: tenant.userId || "",
    createdByName: tenant.userName || "",
    createdByEmail: tenant.userEmail || "",
    createdByRole: tenant.role || "",
    source: resolvedSource,
    ...(EVOLVESA_STOCK_SOURCE_ID ? { sourceId: EVOLVESA_STOCK_SOURCE_ID } : {}),
    raw: payload,
  };
}

function redactEvolveTriggerUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return raw;
  try {
    const parsed = new URL(raw);
    if (parsed.searchParams.has("token")) parsed.searchParams.set("token", "***");
    if (parsed.searchParams.has("apikey")) parsed.searchParams.set("apikey", "***");
    if (parsed.searchParams.has("api_key")) parsed.searchParams.set("api_key", "***");
    return parsed.toString();
  } catch (_) {
    return raw;
  }
}

function parseKeyValueMap(raw) {
  const map = new Map();
  const text = String(raw || "").trim();
  if (!text) return map;
  for (const token of text.split(",")) {
    const pair = token.trim();
    if (!pair) continue;
    const idx = pair.indexOf("=");
    if (idx <= 0 || idx >= pair.length - 1) continue;
    const k = pair.slice(0, idx).trim();
    const v = pair.slice(idx + 1).trim();
    if (k && v) map.set(k, v);
  }
  return map;
}

let EVOLVESA_LEAD_RECEIVING_ENTITY_MAP = parseKeyValueMap(EVOLVESA_LEAD_RECEIVING_ENTITY_MAP_RAW);
let EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER = parseKeyValueMap(EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER_RAW);
let CRM_PROVIDER_BY_DEALER = parseKeyValueMap(CRM_PROVIDER_BY_DEALER_RAW);
let CONSENT_FROM_EMAIL_BY_DEALER = parseKeyValueMap(CONSENT_FROM_EMAIL_BY_DEALER_RAW);
let CRM_PROVIDER_REGISTRY = {};

function buildCrmProviderRegistry() {
  CRM_PROVIDER_REGISTRY = {
    evolvesa: createEvolvesaProvider({
      fetchImpl: fetch,
      uuidv4,
      log,
      canonicalDealerId,
      evolveAuthHeaderValue,
      evolveConfigured,
      toEvolveLeadPayload,
      toEvolveStockPayload,
      redactEvolveTriggerUrl,
      config: {
        baseUrl: EVOLVESA_BASE_URL,
        timeoutMs: EVOLVESA_TIMEOUT_MS,
        stockEndpoint: EVOLVESA_STOCK_ENDPOINT,
        stockTriggerUrl: EVOLVESA_STOCK_TRIGGER_URL,
        leadTriggerUrl: EVOLVESA_LEAD_TRIGGER_URL,
        leadReceivingEntityId: EVOLVESA_LEAD_RECEIVING_ENTITY_ID,
        leadReceivingEntityName: EVOLVESA_LEAD_RECEIVING_ENTITY_NAME,
        leadSource: EVOLVESA_LEAD_SOURCE,
        leadSourceId: EVOLVESA_LEAD_SOURCE_ID,
        stockSourceId: EVOLVESA_STOCK_SOURCE_ID,
        defaultLeadAncillaryArea: EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA,
        defaultLeadUserArea: EVOLVESA_DEFAULT_LEAD_USER_AREA,
        communicationEndpoint: process.env.EVOLVESA_COMMUNICATION_ENDPOINT || "",
      },
      maps: {
        leadReceivingEntityMap: EVOLVESA_LEAD_RECEIVING_ENTITY_MAP,
        leadTriggerUrlByDealer: EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER,
      },
    }),
  };
}

function resolveCrmProvider(tenantContext = {}) {
  const dealerId = canonicalDealerId(tenantContext?.dealerId);
  const dealerPreferred = dealerId ? String(CRM_PROVIDER_BY_DEALER.get(dealerId) || "").trim().toLowerCase() : "";
  const wanted = dealerPreferred || DEFAULT_CRM_PROVIDER;
  return CRM_PROVIDER_REGISTRY[wanted] || CRM_PROVIDER_REGISTRY.evolvesa;
}

buildCrmProviderRegistry();

async function fetchAutoTraderPhotoForVehicle(vehicle = {}) {
  const authHeader = autoTraderAuthHeaderValue();
  if (!authHeader) return null;

  const needleStock = String(vehicle.stockNumber || vehicle.stockNo || "").trim().toLowerCase();
  const needleReg = String(vehicle.registration || vehicle.registrationNumber || vehicle.licenceNumber || "").trim().toLowerCase();
  if (!needleStock && !needleReg) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), AUTOTRADER_TIMEOUT_MS);
  try {
    const response = await fetch(AUTOTRADER_LISTINGS_URL, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: authHeader,
      },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`AutoTrader listings HTTP ${response.status}${text ? `: ${text.slice(0, 300)}` : ""}`);
    }

    let list = [];
    try {
      const json = text ? JSON.parse(text) : [];
      list = Array.isArray(json) ? json : [];
    } catch (_) {
      return null;
    }

    const hit = list.find((row) => {
      const stockNo = String(row?.stockNumber || "").trim().toLowerCase();
      const regNo = String(row?.registrationNumber || "").trim().toLowerCase();
      if (needleStock && stockNo && stockNo === needleStock) return true;
      if (needleReg && regNo && regNo === needleReg) return true;
      return false;
    });
    if (!hit) return null;
    return pickFirstHttpImage(hit.imageUrls);
  } finally {
    clearTimeout(timeout);
  }
}

function normalizeStockRow(row = {}) {
  const imageUrls = Array.isArray(row.imageUrls) ? row.imageUrls : [];
  return {
    stockNumber: row.stockNumber || "",
    registrationNumber: row.registrationNumber || "",
    make: row.make || "",
    model: row.model || "",
    variant: row.variant || "",
    newUsed: row.newUsed || "",
    year: row.year || null,
    mileageInKm: row.mileageInKm || null,
    price: row.price || null,
    colour: row.colour || "",
    bodyType: row.bodyType || "",
    fuelType: row.fuelType || "",
    transmission: row.transmission || "",
    transmissionDrive: row.transmissionDrive || "",
    dealerId: row.dealerId == null ? null : String(row.dealerId),
    vehicleCategory: row.vehicleCategory || "",
    vehicleSubCategory: row.vehicleSubCategory || "",
    description: row.description || "",
    imageUrls,
    primaryImageUrl: pickFirstHttpImage(imageUrls),
    features: Array.isArray(row.features) ? row.features : [],
    raw: row,
  };
}

function normalizeDealerToken(value) {
  let token = String(value || "").trim();
  if (
    (token.startsWith("\"") && token.endsWith("\"")) ||
    (token.startsWith("'") && token.endsWith("'"))
  ) {
    token = token.slice(1, -1).trim();
  }
  return token.toLowerCase();
}

function normalizeDealerDigits(value) {
  return String(value || "").replace(/\D+/g, "");
}

function parseDealerAliases(raw) {
  const map = new Map();
  const text = String(raw || "").trim();
  if (!text) return map;
  // Format: "509=208,ABC=123"
  for (const token of text.split(",")) {
    const pair = token.trim();
    if (!pair) continue;
    const idx = pair.indexOf("=");
    if (idx <= 0 || idx >= pair.length - 1) continue;
    const from = normalizeDealerToken(pair.slice(0, idx));
    const to = normalizeDealerToken(pair.slice(idx + 1));
    if (from && to) map.set(from, to);
  }
  return map;
}

let DEALER_ID_ALIASES = parseDealerAliases(DEALER_ID_ALIASES_RAW);
let USER_EMAIL_DEALER_MAP = parseDealerAliases(USER_EMAIL_DEALER_MAP_RAW);

function tenantConfigSnapshot() {
  return {
    source: TENANT_CONFIG.source,
    dealerAliases: Object.fromEntries(DEALER_ID_ALIASES),
    emailDealerMap: Object.fromEntries(USER_EMAIL_DEALER_MAP),
    defaultCrmProvider: DEFAULT_CRM_PROVIDER,
    crmProviderByDealer: Object.fromEntries(CRM_PROVIDER_BY_DEALER),
    vmgDealerScopes: Array.from(VMG_DEALER_SCOPES),
    vmgStockFeedUrlByDealer: Object.fromEntries(VMG_STOCK_FEED_URL_BY_DEALER),
    evolvesaLeadReceivingEntityMap: Object.fromEntries(EVOLVESA_LEAD_RECEIVING_ENTITY_MAP),
    evolvesaLeadTriggerUrlByDealer: Object.fromEntries(EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER),
  };
}

function readTenantConfigHistory() {
  try {
    if (!fs.existsSync(TENANT_CONFIG_HISTORY_FILE)) return [];
    const parsed = JSON.parse(fs.readFileSync(TENANT_CONFIG_HISTORY_FILE, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function appendTenantConfigHistory(action, payload, req = null) {
  const history = readTenantConfigHistory();
  history.push({
    id: `tconf_${uuidv4()}`,
    action,
    at: new Date().toISOString(),
    actorIp: req?.ip || null,
    actorUserEmail: String(req?.headers?.["x-user-email"] || "").trim().toLowerCase() || null,
    payload,
  });
  const bounded = history.slice(-200);
  fs.writeFileSync(TENANT_CONFIG_HISTORY_FILE, JSON.stringify(bounded, null, 2), "utf8");
  return bounded;
}

function readTenantConfigProposals() {
  try {
    if (!fs.existsSync(TENANT_CONFIG_PROPOSALS_FILE)) return [];
    const parsed = JSON.parse(fs.readFileSync(TENANT_CONFIG_PROPOSALS_FILE, "utf8"));
    return Array.isArray(parsed) ? parsed : [];
  } catch (_) {
    return [];
  }
}

function writeTenantConfigProposals(items) {
  const bounded = Array.isArray(items) ? items.slice(-500) : [];
  fs.writeFileSync(TENANT_CONFIG_PROPOSALS_FILE, JSON.stringify(bounded, null, 2), "utf8");
}

function tenantAdminRole(req) {
  return String(req.headers["x-admin-role"] || req.headers["x-user-role"] || "dealer_principal").trim().toLowerCase();
}

function tenantAdminEmail(req) {
  return String(req.headers["x-admin-user-email"] || req.headers["x-user-email"] || "").trim().toLowerCase();
}

function validateTenantConfigPayload(payload) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "tenant config payload must be a JSON object";
  }
  const maybeMapKeys = [
    "dealerAliases",
    "emailDealerMap",
    "evolvesaLeadReceivingEntityMap",
    "evolvesaLeadTriggerUrlByDealer",
    "vmgStockFeedUrlByDealer",
  ];
  for (const key of maybeMapKeys) {
    const value = payload[key];
    if (value == null) continue;
    if (typeof value !== "object" || Array.isArray(value)) {
      return `${key} must be an object map`;
    }
  }
  if (payload.vmgDealerScopes != null && !Array.isArray(payload.vmgDealerScopes) && typeof payload.vmgDealerScopes !== "string") {
    return "vmgDealerScopes must be an array or comma-separated string";
  }
  return null;
}

function buildTenantConfigPreflightReport(payload) {
  const issues = [];
  const warnings = [];
  const dealerAliases = payload?.dealerAliases && typeof payload.dealerAliases === "object" ? payload.dealerAliases : {};
  const emailDealerMap = payload?.emailDealerMap && typeof payload.emailDealerMap === "object" ? payload.emailDealerMap : {};
  const evolvesaEntityMap =
    payload?.evolvesaLeadReceivingEntityMap && typeof payload.evolvesaLeadReceivingEntityMap === "object"
      ? payload.evolvesaLeadReceivingEntityMap
      : {};
  const evolvesaTriggerMap =
    payload?.evolvesaLeadTriggerUrlByDealer && typeof payload.evolvesaLeadTriggerUrlByDealer === "object"
      ? payload.evolvesaLeadTriggerUrlByDealer
      : {};
  const vmgScopes = parseTokenSet(normalizeConfigList(payload?.vmgDealerScopes));

  for (const [rawFrom, rawTo] of Object.entries(dealerAliases)) {
    const from = normalizeDealerToken(rawFrom);
    const to = normalizeDealerToken(rawTo);
    if (!from || !to) {
      issues.push(`dealerAliases has empty key/value pair: "${rawFrom}"="${rawTo}"`);
      continue;
    }
    if (from === to) warnings.push(`dealerAliases contains no-op mapping: ${from}=${to}`);
  }

  for (const [rawEmail, rawDealer] of Object.entries(emailDealerMap)) {
    const email = String(rawEmail || "").trim().toLowerCase();
    const dealer = normalizeDealerToken(rawDealer);
    if (!email.includes("@")) issues.push(`emailDealerMap has invalid email key: "${rawEmail}"`);
    if (!dealer) issues.push(`emailDealerMap has empty dealer for "${rawEmail}"`);
  }

  for (const [dealer, url] of Object.entries(evolvesaTriggerMap)) {
    const d = normalizeDealerToken(dealer);
    const u = String(url || "").trim();
    if (!d) issues.push(`evolvesaLeadTriggerUrlByDealer has invalid dealer key: "${dealer}"`);
    if (!u.startsWith("http://") && !u.startsWith("https://")) {
      issues.push(`evolvesaLeadTriggerUrlByDealer "${dealer}" must be absolute URL`);
    }
  }

  const vmgFeedMap =
    payload?.vmgStockFeedUrlByDealer && typeof payload.vmgStockFeedUrlByDealer === "object"
      ? payload.vmgStockFeedUrlByDealer
      : {};
  for (const [dealer, url] of Object.entries(vmgFeedMap)) {
    const d = normalizeDealerToken(dealer);
    const u = String(url || "").trim();
    if (!d) issues.push(`vmgStockFeedUrlByDealer has invalid dealer key: "${dealer}"`);
    if (!u.startsWith("http://") && !u.startsWith("https://")) {
      issues.push(`vmgStockFeedUrlByDealer "${dealer}" must be absolute URL`);
    }
  }

  for (const dealer of vmgScopes) {
    if (evolvesaEntityMap[dealer] == null) {
      warnings.push(`vmg dealer scope "${dealer}" has no evolvesaLeadReceivingEntityMap entry`);
    }
  }

  return {
    valid: issues.length === 0,
    issues,
    warnings,
    summary: {
      dealerAliases: Object.keys(dealerAliases).length,
      emailDealerMap: Object.keys(emailDealerMap).length,
      vmgDealerScopes: vmgScopes.size,
      evolvesaLeadReceivingEntityMap: Object.keys(evolvesaEntityMap).length,
      evolvesaLeadTriggerUrlByDealer: Object.keys(evolvesaTriggerMap).length,
      vmgStockFeedUrlByDealer: Object.keys(vmgFeedMap).length,
    },
  };
}

function applyTenantConfigPayload(payload, sourceLabel = "admin_api") {
  const parsed = {
    source: sourceLabel,
    dealerAliasesRaw: normalizeConfigMap(payload.dealerAliases),
    userEmailDealerMapRaw: normalizeConfigMap(payload.emailDealerMap),
    vmgDealerScopesRaw: normalizeConfigList(payload.vmgDealerScopes),
    evolvesaLeadReceivingEntityMapRaw: normalizeConfigMap(payload.evolvesaLeadReceivingEntityMap),
    evolvesaLeadTriggerUrlByDealerRaw: normalizeConfigMap(payload.evolvesaLeadTriggerUrlByDealer),
    vmgStockFeedUrlByDealerRaw: normalizeConfigMap(payload.vmgStockFeedUrlByDealer),
  };
  TENANT_CONFIG = parsed;
  DEALER_ID_ALIASES = parseDealerAliases(parsed.dealerAliasesRaw || DEALER_ID_ALIASES_RAW);
  USER_EMAIL_DEALER_MAP = parseDealerAliases(parsed.userEmailDealerMapRaw || USER_EMAIL_DEALER_MAP_RAW);
  VMG_DEALER_SCOPES = parseTokenSet(parsed.vmgDealerScopesRaw || VMG_DEALER_SCOPES_RAW);
  VMG_STOCK_FEED_URL_BY_DEALER = parseKeyValueMap(
    parsed.vmgStockFeedUrlByDealerRaw || process.env.VMG_STOCK_FEED_URL_BY_DEALER || ""
  );
  EVOLVESA_LEAD_RECEIVING_ENTITY_MAP = parseKeyValueMap(
    parsed.evolvesaLeadReceivingEntityMapRaw || EVOLVESA_LEAD_RECEIVING_ENTITY_MAP_RAW
  );
  EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER = parseKeyValueMap(
    parsed.evolvesaLeadTriggerUrlByDealerRaw || EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER_RAW
  );
  buildCrmProviderRegistry();
}

function requireTenantAdmin(req, res, next) {
  const header = req.headers["authorization"] || "";
  const match = header.match(/^Bearer\s+(.+)$/i);
  const token = match ? match[1] : null;
  if (!token || token !== API_KEY) return res.status(401).json({ error: "unauthorized" });
  if (TENANT_ADMIN_TOKEN) {
    const provided = String(req.headers["x-tenant-admin-token"] || "").trim();
    if (!provided || provided !== TENANT_ADMIN_TOKEN) {
      return res.status(403).json({ error: "forbidden", hint: "missing_or_invalid_tenant_admin_token" });
    }
  }
  req.tenantAdminRole = tenantAdminRole(req);
  req.tenantAdminEmail = tenantAdminEmail(req);
  next();
}

function dealerScopeCandidates(dealerScope) {
  const out = new Set();
  const raw = normalizeDealerToken(dealerScope);
  const digits = normalizeDealerDigits(dealerScope);
  if (raw) out.add(raw);
  if (digits) out.add(digits);
  const aliased = raw ? DEALER_ID_ALIASES.get(raw) : null;
  if (aliased) {
    out.add(aliased);
    const aliasedDigits = normalizeDealerDigits(aliased);
    if (aliasedDigits) out.add(aliasedDigits);
  }
  return out;
}

function stockMatchesDealerScope(row, dealerScope) {
  const wanted = dealerScopeCandidates(dealerScope);
  if (wanted.size === 0) return true;

  const rowDealerRaw = normalizeDealerToken(row?.dealerId);
  if (rowDealerRaw && wanted.has(rowDealerRaw)) return true;

  // Some auth providers send dealer IDs with prefixes/text, while listings are numeric only.
  const rowDigits = normalizeDealerDigits(row?.dealerId);
  if (rowDigits && wanted.has(rowDigits)) return true;

  return false;
}

function normalizeVehicleToken(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeAlphaNum(value) {
  return String(value || "").replace(/[^a-z0-9]/gi, "").toLowerCase();
}

function normalizeLicensePlate(value) {
  return normalizeAlphaNum(value);
}

function normalizeYear(value) {
  const n = Number(String(value || "").trim());
  if (!Number.isFinite(n)) return null;
  const y = Math.floor(n);
  if (y < 1900 || y > 2100) return null;
  return y;
}

function firstNonEmpty(values) {
  for (const v of values) {
    const s = String(v || "").trim();
    if (s) return s;
  }
  return "";
}

function stockRowBranch(row) {
  const raw = row?.raw || {};
  return firstNonEmpty([raw.branchId, raw.branchCode, raw.branch, row?.branchId, row?.branchCode, row?.branch]);
}

function extractStockTakeVehicle(payload = {}) {
  const scan = payload?.scan || {};
  const barcode = payload?.barcode || {};
  const vehicle = payload?.vehicle || payload?.stock || {};
  const driverLicense = payload?.driverLicense || {};
  return {
    vin: firstNonEmpty([vehicle.vin, vehicle.vinNumber, scan.vin, barcode.vin]),
    registration: firstNonEmpty([
      vehicle.registration,
      vehicle.registrationNumber,
      vehicle.licenceNumber,
      vehicle.licensePlate,
      vehicle.licenseNumber,
      scan.registration,
      scan.registrationNumber,
      scan.licenceNumber,
      scan.licensePlate,
      barcode.registration,
      barcode.registrationNumber,
      barcode.licenceNumber,
      barcode.licensePlate,
      barcode.licenseNumber,
      driverLicense.LICENSE_NUMBER,
      driverLicense.licenseNumber,
    ]),
    stockNumber: firstNonEmpty([vehicle.stockNumber, vehicle.stockNo, scan.stockNumber, barcode.stockNumber]),
    make: firstNonEmpty([vehicle.make, scan.make, barcode.make]),
    model: firstNonEmpty([vehicle.model, scan.model, barcode.model]),
    variant: firstNonEmpty([vehicle.variant, scan.variant, barcode.variant]),
    year: normalizeYear(firstNonEmpty([vehicle.year, vehicle.firstRegistrationYear, scan.year, barcode.year])),
    mileage: firstNonEmpty([vehicle.mileage, vehicle.mileageInKm, scan.mileage, barcode.mileage]),
  };
}

function scoreStockTakeMatch(stockRow, scanVehicle) {
  const reasons = [];
  let score = 0;

  const rowRaw = stockRow?.raw || {};
  const rowVin = normalizeAlphaNum(firstNonEmpty([rowRaw.vin, rowRaw.vinNumber, stockRow.vin]));
  const needleVin = normalizeAlphaNum(scanVehicle.vin);
  if (needleVin && rowVin && needleVin === rowVin) {
    score += 120;
    reasons.push("vin_exact");
  }

  const rowReg = normalizeLicensePlate(stockRow.registrationNumber);
  const needleReg = normalizeLicensePlate(scanVehicle.registration);
  if (needleReg && rowReg && needleReg === rowReg) {
    score += 90;
    reasons.push("plate_exact");
  }

  const rowStock = normalizeVehicleToken(stockRow.stockNumber);
  const needleStock = normalizeVehicleToken(scanVehicle.stockNumber);
  if (needleStock && rowStock && needleStock === rowStock) {
    score += 80;
    reasons.push("stock_number_exact");
  }

  const rowMake = normalizeVehicleToken(stockRow.make);
  const rowModel = normalizeVehicleToken(stockRow.model);
  const rowVariant = normalizeVehicleToken(stockRow.variant);
  const needleMake = normalizeVehicleToken(scanVehicle.make);
  const needleModel = normalizeVehicleToken(scanVehicle.model);
  const needleVariant = normalizeVehicleToken(scanVehicle.variant);
  const rowYear = normalizeYear(stockRow.year);
  const needleYear = normalizeYear(scanVehicle.year);

  if (needleMake && rowMake && needleMake === rowMake) {
    score += 20;
    reasons.push("make_exact");
  }
  if (needleModel && rowModel && needleModel === rowModel) {
    score += 20;
    reasons.push("model_exact");
  }
  if (needleVariant && rowVariant && needleVariant === rowVariant) {
    score += 10;
    reasons.push("variant_exact");
  }
  if (needleYear && rowYear && needleYear === rowYear) {
    score += 10;
    reasons.push("year_exact");
  }

  return { score, reasons };
}

function confidenceFromScore(score) {
  if (score >= 100) return "high";
  if (score >= 55) return "medium";
  if (score >= 30) return "low";
  return "none";
}

async function fetchTruTradeReportForStockList(params) {
  const authHeader = truTradeBasicAuthHeaderValue();
  if (!authHeader) {
    throw new Error("Valuation provider not configured");
  }
  const { make, model, variant, year, mileage } = params;
  if (!make || !model || !variant || !year || !mileage) {
    throw new Error("Valuation request requires make, model, variant, year and mileage");
  }
  const baseUrl = (process.env.TRUTRADE_BASE_URL || "https://api.yourvehiclevalue.co.za/api").replace(/\/$/, "");
  const endpoint = new URL(`${baseUrl}/reports`);
  endpoint.searchParams.set("make", String(make));
  endpoint.searchParams.set("model", String(model));
  endpoint.searchParams.set("variant", String(variant));
  endpoint.searchParams.set("year", String(year));
  endpoint.searchParams.set("mileage", String(mileage));

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(process.env.TRUTRADE_TIMEOUT_MS || 15000));
  try {
    const response = await fetch(endpoint.toString(), {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: authHeader,
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let json = null;
    try {
      json = text ? JSON.parse(text) : null;
    } catch (_) {
      json = null;
    }
    if (!response.ok) {
      throw new Error(`Tru-Trade HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
    }
    const report = Array.isArray(json) ? json[0] : (json?.report || json);
    if (!report || typeof report !== "object") {
      throw new Error("Vehicle valuation failed");
    }
    return report;
  } finally {
    clearTimeout(timeout);
  }
}

async function enrichStocksForStockTake(stocks) {
  const list = Array.isArray(stocks) ? stocks : [];
  const out = [];
  // Keep bounded to protect valuation provider and request latency.
  const maxEnriched = Math.min(list.length, 25);
  for (let i = 0; i < list.length; i++) {
    const row = list[i];
    const base = {
      ...row,
      autoTraderPrice: row?.price ?? null,
      tradePrice: null,
      retailPrice: null,
      marketPrice: null,
      valuationStatus: "not_attempted",
    };
    if (i >= maxEnriched) {
      base.valuationStatus = "skipped_limit";
      out.push(base);
      continue;
    }

    const valuationInput = {
      make: firstNonEmpty([row?.make]),
      model: firstNonEmpty([row?.model]),
      variant: firstNonEmpty([row?.variant]),
      year: normalizeYear(firstNonEmpty([row?.year])),
      mileage: firstNonEmpty([row?.mileageInKm]),
    };
    const isComplete = Boolean(
      valuationInput.make &&
      valuationInput.model &&
      valuationInput.variant &&
      valuationInput.year &&
      String(valuationInput.mileage || "").trim()
    );
    if (!isComplete) {
      base.valuationStatus = "missing_fields";
      out.push(base);
      continue;
    }
    try {
      const report = await fetchTruTradeReportForStockList(valuationInput);
      base.tradePrice = report.tradePrice || report.truetrade_tradePrice || null;
      base.retailPrice = report.retailPrice || report.truetrade_retailPrice || null;
      base.marketPrice = report.marketPrice || report.truetrade_marketPrice || null;
      base.valuationStatus = "ok";
    } catch (_) {
      base.valuationStatus = "valuation_failed";
    }
    out.push(base);
  }
  return out;
}

async function fetchAutoTraderListingsCached(forceRefresh = false) {
  const authHeader = autoTraderAuthHeaderValue();
  if (!authHeader) {
    throw new Error("AutoTrader syndication is not configured");
  }
  const now = Date.now();
  if (autoTraderRateLimitedUntilMs > now && autoTraderListingsCache.rows.length > 0) {
    return {
      rows: autoTraderListingsCache.rows,
      source: "rate_limit_cache",
      cachedAt: new Date(autoTraderListingsCache.fetchedAtMs).toISOString(),
    };
  }
  if (!forceRefresh && autoTraderListingsCache.rows.length > 0 && now - autoTraderListingsCache.fetchedAtMs < AUTOTRADER_LISTINGS_CACHE_TTL_MS) {
    return {
      rows: autoTraderListingsCache.rows,
      source: "cache",
      cachedAt: new Date(autoTraderListingsCache.fetchedAtMs).toISOString(),
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), AUTOTRADER_TIMEOUT_MS);
  try {
    const response = await fetch(AUTOTRADER_LISTINGS_URL, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: authHeader,
      },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      if (response.status === 429) {
        autoTraderRateLimitedUntilMs = Date.now() + AUTOTRADER_RATE_LIMIT_COOLDOWN_MS;
      }
      throw new Error(`AutoTrader listings HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
    }
    let json = [];
    try {
      json = text ? JSON.parse(text) : [];
    } catch (_) {
      json = [];
    }
    const list = Array.isArray(json) ? json : [];
    const normalized = list.map(normalizeStockRow);
    autoTraderListingsCache.rows = normalized;
    autoTraderListingsCache.fetchedAtMs = Date.now();
    autoTraderRateLimitedUntilMs = 0;
    persistAutoTraderCache();
    return {
      rows: normalized,
      source: "upstream",
      cachedAt: new Date(autoTraderListingsCache.fetchedAtMs).toISOString(),
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchVmgListingsCached(forceRefresh = false, dealerScope = "") {
  const feedUrl = resolveVmgStockFeedUrl(dealerScope);
  if (!feedUrl) {
    throw new Error("VMG stock feed is not configured for this dealership");
  }
  const bucket = getVmgListingsBucket(feedUrl);
  const now = Date.now();
  if (!forceRefresh && bucket.rows.length > 0 && now - bucket.fetchedAtMs < VMG_CACHE_TTL_MS) {
    return {
      rows: bucket.rows,
      source: "vmg_cache",
      cachedAt: new Date(bucket.fetchedAtMs).toISOString(),
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), VMG_TIMEOUT_MS);
  try {
    const response = await fetch(feedUrl, {
      method: "GET",
      headers: { Accept: "application/xml,text/xml,*/*" },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`VMG stock feed HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
    }
    const normalized = parseVmgStockXml(text);
    bucket.rows = normalized;
    bucket.fetchedAtMs = Date.now();
    persistVmgCache();
    return {
      rows: normalized,
      source: "vmg_upstream",
      cachedAt: new Date(bucket.fetchedAtMs).toISOString(),
    };
  } finally {
    clearTimeout(timeout);
  }
}

/**
 * Adapter stubs for each CRM/DMS.
 * For now, we just simulate successful processing.
 */
async function processCommand(commandType, payload) {
  /**
   * v1 stub adapter layer:
   * - Returns synthetic IDs so the mobile workflow can chain commands.
   * - Later we will replace these with real EvolveSA/CMS/Keyloop/VMG adapters.
   */
  const now = new Date().toISOString();

  const getByCorrelation = (correlationId) => {
    if (!correlationId) return null;
    return commands.get(String(correlationId)) || null;
  };

  const resolveLeadId = (payload) => {
    if (payload?.leadId) return payload.leadId;
    const leadCmd = getByCorrelation(payload?.leadCorrelationId);
    return leadCmd?.result?.leadId || null;
  };

  const resolveStockId = (payload) => {
    if (payload?.stockUnitId) return payload.stockUnitId;
    const stockCmd = getByCorrelation(payload?.stockCorrelationId);
    return stockCmd?.result?.stockUnitId || null;
  };

  const scoreBandFor = (score) => {
    const s = Number(score);
    if (!Number.isFinite(s)) return "unknown";
    if (s >= 767) return "excellent";
    if (s >= 681) return "good";
    if (s >= 614) return "favourable";
    if (s >= 583) return "average";
    if (s >= 527) return "below_average";
    if (s >= 487) return "unfavourable";
    return "poor";
  };

  const decisionForBand = (band) => {
    if (["excellent", "good", "favourable", "average"].includes(band)) return "proceed";
    if (band === "below_average") return "manager_review";
    return "decline";
  };

  const buildCreditQuickRequest = (payload, leadId) => {
    const applicant = payload?.applicant || {};
    return {
      name: String(applicant.firstName || applicant.name || "").trim(),
      surname: String(applicant.surname || applicant.lastName || "").trim(),
      contact: String(applicant.mobile || applicant.contact || payload.phone || "").trim(),
      email: String(applicant.email || payload.email || "").trim(),
      id: String(applicant.idNumber || payload.idNumber || "").trim(),
      appId: String(leadId || "").trim(),
    };
  };

  const runCreditCheck = async (payload) => {
    const leadId = resolveLeadId(payload);
    if (!leadId) {
      throw new Error("leadId is required (or leadCorrelationId that maps to a CREATE_LEAD)");
    }
    const consentId = String(payload?.consentId || "").trim();
    const inlineConsent = payload?.consent;
    const inlineAccepted =
      isPlainObject(inlineConsent) &&
      (inlineConsent.accepted === true || String(inlineConsent.accepted || "").toLowerCase() === "true");
    let approvedConsent;
    if (consentId) {
      approvedConsent = resolveApprovedConsent(payload, payload?._tenantContext || {});
    } else if (inlineAccepted) {
      if (CREDIT_CHECK_MODE !== "stub") {
        throw new Error(
          "CREDIT_CHECK without consentId is only supported in CREDIT_CHECK_MODE=stub; use email consent or add a provider adapter."
        );
      }
      approvedConsent = { consentId: null };
    } else {
      throw new Error("consentId is required before soft credit check");
    }
    const request = buildCreditQuickRequest(payload, leadId);
    if (CREDIT_CHECK_MODE !== "stub") {
      throw new Error(`Unsupported CREDIT_CHECK_MODE=${CREDIT_CHECK_MODE}. Configure provider adapter or set stub.`);
    }
    const digits = String(request.id || "").replace(/\D+/g, "");
    const seed = digits.split("").reduce((acc, d, idx) => acc + Number(d || 0) * (idx + 3), 0);
    const score = Math.max(0, Math.min(999, 450 + (seed % 360)));
    const band = scoreBandFor(score);
    return {
      mode: "stub",
      provider: "CPB",
      leadId: String(leadId),
      score,
      band,
      decision: decisionForBand(band),
      providerCode: "000",
      providerRef: `stub_${uuidv4()}`,
      consentId: approvedConsent.consentId,
      request,
      response: {
        response_status: "Success",
        result: {
          CreditScoreList: [{ CreditScore: score, CreditScoreCategory: band, ScoreDate: new Date().toISOString().slice(0, 10) }],
          status_message: "OK",
          bh_response_code: "000",
          http_code: "200",
          request_reference: `stub_${uuidv4()}`,
        },
        score,
      },
    };
  };

  const result = { processedAt: now };

  function truTradeBasicAuthToken() {
    // Some accounts use portal username/password; API docs use apiKey:apiSecret — support both.
    const u = process.env.TRUTRADE_AUTH_USERNAME;
    const p = process.env.TRUTRADE_AUTH_PASSWORD;
    if (u && p) {
      return Buffer.from(`${u}:${p}`).toString("base64");
    }
    const apiKey = process.env.TRUTRADE_API_KEY;
    const apiSecret = process.env.TRUTRADE_API_SECRET;
    if (!apiKey || !apiSecret) {
      return null;
    }
    return Buffer.from(`${apiKey}:${apiSecret}`).toString("base64");
  }

  async function fetchTruTradeReport(params) {
    const authToken = truTradeBasicAuthToken();
    if (!authToken) {
      throw new Error(
        "Valuation provider not configured: set TRUTRADE_API_KEY + TRUTRADE_API_SECRET, " +
        "or TRUTRADE_AUTH_USERNAME + TRUTRADE_AUTH_PASSWORD in connector .env, then restart npm start"
      );
    }

    const baseUrl = process.env.TRUTRADE_BASE_URL || "https://api.yourvehiclevalue.co.za/api";
    const { make, model, variant, year, mileage } = params;

    if (!make || !model || !variant || !year || !mileage) {
      throw new Error("Valuation request requires make, model, variant, year and mileage");
    }

    const base = baseUrl.replace(/\/$/, "");
    const headers = {
      "Accept": "application/json",
      "Authorization": `Basic ${authToken}`,
    };

    const normalizeKey = (v) => String(v || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    const toList = (json) => {
      if (Array.isArray(json)) return json;
      if (Array.isArray(json?.data)) return json.data;
      if (Array.isArray(json?.results)) return json.results;
      if (Array.isArray(json?.items)) return json.items;
      return [];
    };
    const candidateText = (x) => {
      if (x == null) return "";
      if (typeof x === "string" || typeof x === "number") return String(x);
      // Support multiple vendor response shapes (name/label/value, code/year ids, etc.).
      return String(
        x.name ||
        x.label ||
        x.value ||
        x.make ||
        x.model ||
        x.variant ||
        x.year ||
        x.code ||
        x.id ||
        x.description ||
        x.title ||
        ""
      );
    };
    const pickBest = (list, wanted, label) => {
      const w = normalizeKey(wanted);
      const mapped = list.map((x) => ({ raw: x, text: candidateText(x), key: normalizeKey(candidateText(x)) }))
        .filter((x) => x.key.length > 0);
      if (mapped.length === 0) throw new Error(`Valuation lookup returned no ${label} options`);
      const exact = mapped.find((x) => x.key === w);
      if (exact) return exact.text;
      const startsWith = mapped.filter((x) => x.key.startsWith(w) || w.startsWith(x.key));
      if (startsWith.length > 0) {
        startsWith.sort((a, b) => b.key.length - a.key.length);
        return startsWith[0].text;
      }
      const contains = mapped
        .filter((x) => x.key.length >= 3)
        .filter((x) => x.key.includes(w) || w.includes(x.key));
      if (contains.length > 0) {
        contains.sort((a, b) => b.key.length - a.key.length);
        return contains[0].text;
      }
      throw new Error(`Valuation lookup could not match ${label} '${wanted}'`);
    };

    const getJson = async (url) => {
      const response = await fetch(url, { method: "GET", headers });
      const text = await response.text();
      if (process.env.TRUTRADE_DEBUG_LOG === "1") {
        console.error(`[Valuation] GET ${url.replace(/\/\/[^/]+/, "//***")} -> HTTP ${response.status}`);
      }
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch (_) {
        json = null;
      }
      if (!response.ok) {
        const preview = text.length > 800 ? text.slice(0, 800) + "…" : text;
        throw new Error(`Valuation provider HTTP ${response.status}: ${preview}`);
      }
      return json;
    };
    const getJsonSoft = async (url) => {
      try {
        return await getJson(url);
      } catch (_) {
        return null;
      }
    };

    const rawMake = String(make || "").trim();
    const rawModel = String(params.rawModel || model || "").trim();
    const rawVariant = String(params.rawVariant || variant || "").trim();
    let resolvedMake = rawMake;
    let resolvedModel = rawModel;
    let resolvedVariant = rawVariant;
    let resolvedYear = String(year);
    let usedLookupNormalization = false;

    try {
      // Normalize values using valuation lookup workflow when available.
      const makeList = toList(await getJson(`${base}/lookup/make`));
      resolvedMake = pickBest(makeList, make, "make");

      const modelList = toList(await getJson(`${base}/lookup/model/${encodeURIComponent(resolvedMake)}`));
      resolvedModel = pickBest(modelList, model, "model");

      const yearList = toList(await getJson(`${base}/lookup/year/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(resolvedModel)}`));
      resolvedYear = pickBest(yearList, String(year), "year");

      // Variant lookup endpoint differs by tenant; prefer /lookup/variant and fallback to /lookup/year/.../year.
      let variantJson = await getJsonSoft(
        `${base}/lookup/variant/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(resolvedModel)}/${encodeURIComponent(String(resolvedYear))}`
      );
      if (!variantJson) {
        variantJson = await getJson(
          `${base}/lookup/year/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(resolvedModel)}/${encodeURIComponent(String(resolvedYear))}`
        );
      }
      const variantList = toList(variantJson);
      resolvedVariant = pickBest(variantList, variant, "variant");
      usedLookupNormalization = true;
    } catch (lookupError) {
      // Some valuation provider environments do not expose all lookup endpoints.
      // Fall back to direct valuation with best values we already resolved.
      log("warn", "trutrade_lookup_unavailable_fallback_report", {
        error: lookupError?.message || String(lookupError),
        partialResolvedMake: resolvedMake,
        partialResolvedModel: resolvedModel,
      });
      // Keep resolvedMake/resolvedModel if lookup provided them;
      // only variant/year stay as user-provided when those lookups fail.
      resolvedVariant = rawVariant;
      resolvedYear = String(year);
    }

    let fallbackModelCandidates = [];
    if (!usedLookupNormalization) {
      try {
        const modelJson = await getJsonSoft(`${base}/lookup/model/${encodeURIComponent(resolvedMake)}`);
        const modelList = toList(modelJson)
          .map(candidateText)
          .map((x) => String(x || "").trim())
          .filter((x) => x.length > 0);
        const rawNeedle = normalizeKey(rawModel || resolvedModel);
        const words = String(rawModel || resolvedModel)
          .toLowerCase()
          .split(/[^a-z0-9]+/)
          .filter((w) => w.length >= 3);
        const ranked = modelList
          .map((m) => {
            const k = normalizeKey(m);
            const scoreContains = rawNeedle && (k.includes(rawNeedle) || rawNeedle.includes(k)) ? 3 : 0;
            const scoreWords = words.reduce((acc, w) => acc + (k.includes(w) ? 1 : 0), 0);
            return { model: m, score: scoreContains + scoreWords };
          })
          .filter((x) => x.score > 0)
          .sort((a, b) => b.score - a.score || b.model.length - a.model.length)
          .slice(0, 5)
          .map((x) => x.model);
        fallbackModelCandidates = ranked;
      } catch (_) {
        fallbackModelCandidates = [];
      }
    }

    let fallbackVariantCandidates = [];
    if (!usedLookupNormalization) {
      try {
        const modelSet = Array.from(new Set([
          resolvedModel,
          rawModel,
          ...fallbackModelCandidates,
        ].map((x) => String(x || "").trim()).filter((x) => x.length >= 2)));
        const variantSet = new Set();
        for (const m of modelSet.slice(0, 5)) {
          const vJson = await getJsonSoft(
            `${base}/lookup/variant/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(m)}/${encodeURIComponent(String(resolvedYear))}`
          );
          const values = toList(vJson)
            .map(candidateText)
            .map((x) => String(x || "").trim())
            .filter((x) => x.length > 0);
          for (const v of values) variantSet.add(v);
        }
        fallbackVariantCandidates = Array.from(variantSet).slice(0, 12);
      } catch (_) {
        fallbackVariantCandidates = [];
      }
    }

    // Prefer insight endpoint (when lookups worked); keep direct report fallback.
    const valuationEndpoints = usedLookupNormalization
      ? [
          `${base}/insight/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(resolvedModel)}/${encodeURIComponent(resolvedVariant)}/${encodeURIComponent(String(resolvedYear))}`,
          `${base}/report/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(resolvedModel)}/${encodeURIComponent(resolvedVariant)}/${encodeURIComponent(String(resolvedYear))}/${encodeURIComponent(String(mileage))}`,
        ]
      : [
          // Direct mode with heuristic fallbacks for scan-derived strings.
          ...(() => {
            const candidates = [];
            const addCandidate = (m, v) => {
              const key = `${m}|||${v}`;
              if (!m || !v) return;
              if (candidates.some((c) => c.key === key)) return;
              candidates.push({ key, model: m, variant: v });
            };

            const makeText = String(resolvedMake || rawMake).trim();
            const modelText = String(resolvedModel || rawModel).trim();
            const variantText = String(resolvedVariant || rawVariant).trim();
            const rawModelText = rawModel;
            const rawVariantText = rawVariant;
            const makeUpper = makeText.toUpperCase();
            const stripMakePrefix = (value) => {
              const raw = String(value || "").trim();
              if (!raw) return raw;
              const upper = raw.toUpperCase();
              if (upper.startsWith(`${makeUpper} `)) {
                return raw.substring(makeText.length).trim();
              }
              return raw;
            };

            // 1) Normalized/lookup values first.
            addCandidate(modelText, variantText);
            // 1b) Always include raw app values too.
            addCandidate(rawModelText, rawVariantText);
            // 1c) Add likely model candidates from /lookup/model/{make}.
            for (const m of fallbackModelCandidates) {
              addCandidate(m, variantText);
              addCandidate(m, rawVariantText);
            }
            // 1d) If we have catalog variants from lookup, try them too.
            for (const m of [modelText, rawModelText, ...fallbackModelCandidates]) {
              for (const v of fallbackVariantCandidates) {
                addCandidate(m, v);
              }
            }

            // 2) Remove duplicated make prefix from model/variant.
            const modelNoMake = stripMakePrefix(modelText);
            const variantNoMake = stripMakePrefix(variantText);
            const rawModelNoMake = stripMakePrefix(rawModelText);
            const rawVariantNoMake = stripMakePrefix(rawVariantText);
            addCandidate(modelNoMake, variantNoMake);
            addCandidate(rawModelNoMake, rawVariantNoMake);

            // 3) If model and variant are identical full strings, split into model token + variant remainder.
            if (modelNoMake.toUpperCase() === variantNoMake.toUpperCase()) {
              const parts = modelNoMake.split(/\s+/).filter(Boolean);
              if (parts.length >= 2) {
                const modelBase = parts[0];
                const variantRemainder = parts.slice(1).join(" ");
                addCandidate(modelBase, variantRemainder);
              }
            }
            if (rawModelNoMake.toUpperCase() === rawVariantNoMake.toUpperCase()) {
              const parts = rawModelNoMake.split(/\s+/).filter(Boolean);
              if (parts.length >= 2) {
                const modelBase = parts[0];
                const variantRemainder = parts.slice(1).join(" ");
                addCandidate(modelBase, variantRemainder);
              }
            }

            // 4) If variant still starts with model text, drop model from variant.
            if (variantNoMake.toUpperCase().startsWith(`${modelNoMake.toUpperCase()} `)) {
              const compactVariant = variantNoMake.substring(modelNoMake.length).trim();
              addCandidate(modelNoMake, compactVariant);
            }
            if (rawVariantNoMake.toUpperCase().startsWith(`${rawModelNoMake.toUpperCase()} `)) {
              const compactVariant = rawVariantNoMake.substring(rawModelNoMake.length).trim();
              addCandidate(rawModelNoMake, compactVariant);
            }

            // 5) Ignore too-short model tokens from vendor lookup codes in fallback URLs.
            const filtered = candidates.filter((c) => c.model.trim().length >= 2);
            const expanded = [];
            const seen = new Set();
            const addExpanded = (m, v) => {
              const key = `${m}|||${v}`;
              if (seen.has(key)) return;
              seen.add(key);
              expanded.push({ model: m, variant: v });
            };
            for (const c of filtered) {
              addExpanded(c.model, c.variant);
              // Some provider path parameters can choke on "/" inside variants (e.g. A/T).
              if (c.variant.includes("/")) {
                addExpanded(c.model, c.variant.replace(/\//g, " "));
                addExpanded(c.model, c.variant.replace(/\//g, ""));
                addExpanded(c.model, c.variant.replace(/A\/T/gi, "AT"));
                addExpanded(c.model, c.variant.replace(/A\/T/gi, "AUTO"));
              }
            }

            return expanded.flatMap((c) => ([
              `${base}/insight/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(c.model)}/${encodeURIComponent(c.variant)}/${encodeURIComponent(String(resolvedYear))}`,
              `${base}/report/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(c.model)}/${encodeURIComponent(c.variant)}/${encodeURIComponent(String(resolvedYear))}/${encodeURIComponent(String(mileage))}`,
            ]));
          })(),
        ];

    let lastError = null;
    for (const valuationUrl of valuationEndpoints) {
      try {
        const json = await getJson(valuationUrl);
        return {
          ...(json || {}),
          resolvedInput: {
            make: resolvedMake,
            model: resolvedModel,
            variant: resolvedVariant,
            year: String(resolvedYear),
            mileage: String(mileage),
          },
          lookupNormalizationUsed: usedLookupNormalization,
        };
      } catch (e) {
        lastError = e;
      }
    }
    if (lastError) {
      const lower = String(lastError.message || lastError).toLowerCase();
      const uniqueTexts = (list) => {
        const seen = new Set();
        const out = [];
        for (const item of list) {
          const t = String(item || "").trim();
          if (!t) continue;
          const k = t.toLowerCase();
          if (seen.has(k)) continue;
          seen.add(k);
          out.push(t);
        }
        return out;
      };
      const stripMakePrefixGeneric = (makeValue, textValue) => {
        const makeClean = String(makeValue || "").trim();
        const textClean = String(textValue || "").trim();
        if (!makeClean || !textClean) return textClean;
        if (textClean.toUpperCase().startsWith(`${makeClean.toUpperCase()} `)) {
          return textClean.substring(makeClean.length).trim();
        }
        return textClean;
      };

      if (lower.includes("invalid variant information")) {
        const modelCandidates = uniqueTexts([
          resolvedModel,
          rawModel,
          stripMakePrefixGeneric(resolvedMake, resolvedModel),
          stripMakePrefixGeneric(rawMake, rawModel),
        ]).filter((x) => x.length >= 2);

        let variants = [];
        for (const m of modelCandidates) {
          const lookupUrlVariant = `${base}/lookup/variant/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(m)}/${encodeURIComponent(String(resolvedYear))}`;
          const lookupUrlYear = `${base}/lookup/year/${encodeURIComponent(resolvedMake)}/${encodeURIComponent(m)}/${encodeURIComponent(String(resolvedYear))}`;
          let vJson = await getJsonSoft(lookupUrlVariant);
          if (!vJson) vJson = await getJsonSoft(lookupUrlYear);
          const vList = toList(vJson).map(candidateText).map((x) => String(x || "").trim()).filter((x) => x.length > 0);
          if (vList.length > 0) {
            variants = uniqueTexts(vList).slice(0, 12);
            break;
          }
        }
        if (variants.length > 0) {
          lastError.variantSuggestions = variants;
        }
      }

      if (lower.includes("invalid model information")) {
        const modelJson = await getJsonSoft(`${base}/lookup/model/${encodeURIComponent(resolvedMake)}`);
        const modelList = toList(modelJson).map(candidateText).map((x) => String(x || "").trim()).filter((x) => x.length > 0);
        const models = uniqueTexts(modelList).slice(0, 12);
        if (models.length > 0) {
          lastError.modelSuggestions = models;
        }
      }
    }
    throw lastError || new Error("Vehicle valuation failed");
  }

  async function sendWhatsAppViaTwilio(opts) {
    const accountSid = process.env.TWILIO_ACCOUNT_SID;
    const authToken = process.env.TWILIO_AUTH_TOKEN;
    const whatsappFrom = process.env.TWILIO_WHATSAPP_FROM;

    if (!accountSid || !authToken || !whatsappFrom) {
      throw new Error("Twilio not configured (missing TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_WHATSAPP_FROM)");
    }

    const toRaw = opts?.to;
    if (!toRaw || typeof toRaw !== "string") {
      throw new Error("WhatsApp 'to' number is required");
    }

    const to = toRaw.startsWith("whatsapp:") ? toRaw : `whatsapp:${toRaw}`;
    const from = whatsappFrom.startsWith("whatsapp:") ? whatsappFrom : `whatsapp:${whatsappFrom}`;

    const body = String(opts?.body || "").trim();
    if (!body) throw new Error("WhatsApp 'body' is required");

    const mediaUrl = opts?.mediaUrl ? String(opts.mediaUrl).trim() : null;

    const url = `https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Messages.json`;
    const form = new URLSearchParams();
    form.append("To", to);
    form.append("From", from);
    form.append("Body", body);
    if (mediaUrl) form.append("MediaUrl", mediaUrl);

    const basicAuth = Buffer.from(`${accountSid}:${authToken}`).toString("base64");

    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Authorization": `Basic ${basicAuth}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body: form
    });

    const text = await response.text();
    if (!response.ok) {
      throw new Error(`Twilio WhatsApp send failed: HTTP ${response.status} ${text}`);
    }

    const json = JSON.parse(text);
    return {
      sid: json.sid,
      status: json.status,
    };
  }

  switch (commandType) {
    case "CREATE_LEAD": {
      const provider = resolveCrmProvider(payload?._tenantContext || {});
      const lead = await provider.createLead(payload);
      const tc = payload?._tenantContext || {};
      result.leadId = lead.leadId;
      result.provider = lead.provider || provider.providerName || "EvolveSA";
      result.mode = lead.mode;
      if (lead.warning) result.warning = lead.warning;
      if (lead.rawRef) result.rawRef = lead.rawRef;
      if (lead.debug) result.evolvesaDebug = lead.debug;
      result.lead = payload?.driverLicense || payload?.lead || {};
      result.leadOwner = {
        ownerUserId: tc.userId || null,
        ownerEmail: String(tc.userEmail || "").trim().toLowerCase() || null,
        ownerName: tc.userName || null,
        ownerRole: normalizeRole(tc.role),
        dealerId: String(tc.dealerId || "").trim() || null,
        branchId: String(tc.branchId || "").trim() || null,
      };
      return result;
    }
    case "CREDIT_CHECK": {
      const credit = await runCreditCheck(payload);
      result.leadId = credit.leadId;
      result.provider = credit.provider;
      result.mode = credit.mode;
      result.consentId = credit.consentId || String(payload?.consentId || "").trim() || null;
      result.creditCheck = {
        score: credit.score,
        band: credit.band,
        decision: credit.decision,
        providerCode: credit.providerCode,
        providerRef: credit.providerRef,
      };
      if (result.consentId && consentRecords.has(result.consentId)) {
        const rec = consentRecords.get(result.consentId);
        rec.usedAt = new Date().toISOString();
        rec.creditCheckCorrelationId = String(payload?._commandCorrelationId || "");
        rec.updatedAt = new Date().toISOString();
        consentRecords.set(rec.consentId, rec);
        appendConsentEvent(rec.consentId, "used_for_credit_check", {
          leadId: result.leadId,
        });
        persistStore();
      }
      result.creditCheckDebug = {
        request: credit.request,
        response: credit.response,
      };
      return result;
    }
    case "SHARE_LEAD": {
      const leadId = resolveLeadId(payload);
      if (!leadId) throw new Error("leadId is required (or leadCorrelationId that maps to a CREATE_LEAD)");
      result.leadId = leadId;
      result.shareId = `share_${uuidv4()}`;
      result.shareTarget = payload?.target || payload?.shareTarget || "UNKNOWN";
      return result;
    }
    case "LOG_COMMUNICATION": {
      const leadId = resolveLeadId(payload);
      if (!leadId) throw new Error("leadId is required (or leadCorrelationId that maps to a CREATE_LEAD)");
      const provider = resolveCrmProvider(payload?._tenantContext || {});
      const providerPayload = {
        ...payload,
        path: `/leads/leads/${leadId}`,
        entityId: String(leadId),
      };
      const communication = await provider.logCommunication({
        ...providerPayload,
        leadId,
        _serverNowIso: now,
      });
      result.leadId = leadId;
      result.provider = provider.providerName || "EvolveSA";
      result.communication = communication || {};
      return result;
    }
    case "CREATE_STOCK_UNIT": {
      const provider = resolveCrmProvider(payload?._tenantContext || {});
      const stock = await provider.createStockUnit(payload);
      result.stockUnitId = stock.stockUnitId;
      result.provider = stock.provider || provider.providerName || "EvolveSA";
      result.mode = stock.mode;
      if (stock.warning) result.warning = stock.warning;
      if (stock.rawRef) result.rawRef = stock.rawRef;
      if (stock.debug) result.evolvesaDebug = stock.debug;
      result.vehicle = payload?.vehicle || payload?.stock || {};
      return result;
    }
    case "SEND_STOCK_TO_LEAD": {
      const leadId = resolveLeadId(payload);
      const stockUnitId = resolveStockId(payload);
      if (!leadId) throw new Error("leadId is required (or leadCorrelationId)");
      if (!stockUnitId) throw new Error("stockUnitId is required (or stockCorrelationId)");
      result.leadId = leadId;
      result.stockUnitId = stockUnitId;
      result.dispatchId = `dispatch_${uuidv4()}`;
      // Optional: send a WhatsApp message to the lead.
      // Mobile provides recipient number + optional photo URL + optional link.
      const vehicle = payload?.vehicle || payload?.stock || {};
      const reg = vehicle.registration || vehicle.reg || "";
      const make = vehicle.make || "";
      const model = vehicle.model || "";
      const vin = vehicle.vin || "";
      const expiry = vehicle.expiry || "";

      const whatsapp = payload?.whatsapp || {};
      const to = whatsapp?.to || whatsapp?.phone || payload?.whatsappTo;
      let photoUrl = whatsapp?.mediaUrl || whatsapp?.photoUrl || payload?.photoUrl || null;
      if (!photoUrl) {
        try {
          photoUrl = await fetchAutoTraderPhotoForVehicle(vehicle);
          if (photoUrl) {
            result.photoSource = "autotrader_syndication";
            result.photoUrl = photoUrl;
          }
        } catch (e) {
          result.photoLookupWarning = e?.message || String(e);
        }
      }
      const dmsLink = whatsapp?.link || payload?.dmsLink || "";

      if (to) {
        const body =
          `Hi! Your stock unit has been prepared.\n` +
          `Stock ID: ${stockUnitId}\n` +
          (reg ? `Registration: ${reg}\n` : "") +
          (vin ? `VIN: ${vin}\n` : "") +
          (make || model ? `Vehicle: ${make} ${model}\n` : "") +
          (expiry ? `Expiry: ${expiry}\n` : "") +
          (dmsLink ? `View details: ${dmsLink}\n` : "");

        try {
          const twilio = await sendWhatsAppViaTwilio({
            to,
            mediaUrl: photoUrl,
            body,
          });

          result.whatsapp = {
            to,
            messageSid: twilio.sid,
            status: twilio.status,
          };
        } catch (e) {
          result.whatsapp = {
            to,
            skipped: false,
            error: e?.message || String(e),
          };
        }
      } else {
        result.whatsapp = { skipped: true, reason: "no whatsapp.to provided" };
      }

      return result;
    }
    case "TRADE_IN": {
      const stockUnitId = resolveStockId(payload);
      const damages = Array.isArray(payload?.damages) ? payload.damages : [];
      const reconEstimateTotal = damages.reduce((sum, item) => {
        const n = Number(item?.reconCost || 0);
        return Number.isFinite(n) && n >= 0 ? sum + n : sum;
      }, 0);
      result.stockUnitId = stockUnitId;
      result.tradeInId = `tradein_${uuidv4()}`;
      result.tradeIn = payload || {};
      result.damageAssessment = {
        photos: payload?.photos || {},
        wireframe: payload?.damageWireframe || { markers: [] },
        damages,
        reconEstimateTotal,
        currency: payload?.currency || "ZAR",
      };
      return result;
    }
    case "STOCK_TAKE": {
      const tenant = payload?._tenantContext || {};
      const dealerScope = String(tenant?.dealerId || "").trim();
      const branchScope = String(tenant?.branchId || "").trim();
      const scanVehicle = extractStockTakeVehicle(payload || {});
      const scanHasIdentity =
        Boolean(normalizeAlphaNum(scanVehicle.vin)) ||
        Boolean(normalizeLicensePlate(scanVehicle.registration)) ||
        Boolean(normalizeVehicleToken(scanVehicle.stockNumber)) ||
        (Boolean(normalizeVehicleToken(scanVehicle.make)) && Boolean(normalizeVehicleToken(scanVehicle.model)));

      result.stockTakeId = `stocktake_${uuidv4()}`;
      result.stockTake = payload || {};
      result.match = null;
      result.possibleMatches = [];

      if (!dealerScope) {
        result.warning = "Missing dealer scope; stock match skipped.";
        return result;
      }
      if (!scanHasIdentity) {
        result.warning = "No usable barcode/vehicle identity fields found for stock matching.";
        return result;
      }

      const useVmgFeed = useVmgFeedForDealer(dealerScope);
      let fetched;
      try {
        fetched = useVmgFeed
          ? await fetchVmgListingsCached(false, dealerScope)
          : await fetchAutoTraderListingsCached(false);
      } catch (e) {
        fetched = {
          rows: useVmgFeed
            ? vmgCachedRowsForDealerScope(dealerScope)
            : (Array.isArray(autoTraderListingsCache.rows) ? autoTraderListingsCache.rows : []),
          source: "cache_only_on_error",
          cachedAt: useVmgFeed
            ? (() => {
                const b = getVmgListingsBucket(resolveVmgStockFeedUrl(dealerScope));
                return b.fetchedAtMs ? new Date(b.fetchedAtMs).toISOString() : null;
              })()
            : (autoTraderListingsCache.fetchedAtMs ? new Date(autoTraderListingsCache.fetchedAtMs).toISOString() : null),
        };
        result.stockLookupWarning = e?.message || String(e);
      }

      const dealerRows = useVmgFeed
        ? fetched.rows
        : fetched.rows.filter((row) => stockMatchesDealerScope(row, dealerScope));
      log("info", "stock_take_feed_selected", {
        provider: useVmgFeed ? "vmg" : "autotrader",
        dealerScope,
        rowsLoaded: Array.isArray(fetched.rows) ? fetched.rows.length : 0,
        rowsAfterScope: dealerRows.length,
        source: fetched.source || null,
      });
      const scored = dealerRows
        .map((row) => {
          const match = scoreStockTakeMatch(row, scanVehicle);
          return {
            row,
            score: match.score,
            reasons: match.reasons,
            branch: stockRowBranch(row),
          };
        })
        .filter((x) => x.score > 0)
        .sort((a, b) => b.score - a.score);

      const branchExact = branchScope
        ? scored.filter((x) => String(x.branch || "").trim() === branchScope)
        : scored;
      const branchOverlap = branchScope
        ? scored.filter((x) => x.branch && String(x.branch || "").trim() !== branchScope)
        : [];
      const ordered = branchExact.concat(branchOverlap);

      const top = ordered[0] || null;
      result.stockLookup = {
        source: fetched.source,
        cachedAt: fetched.cachedAt,
        dealerScope: dealerScope || null,
        branchScope: branchScope || null,
        totalDealerRows: dealerRows.length,
        matchedCount: ordered.length,
        branchOverlapCount: branchOverlap.length,
      };

      result.possibleMatches = ordered.slice(0, 5).map((item) => ({
        confidence: confidenceFromScore(item.score),
        score: item.score,
        reasons: item.reasons,
        branchId: item.branch || null,
        stock: item.row,
      }));

      if (!top) {
        result.warning = `No ${useVmgFeed ? "VMG" : "AutoTrader"} stock match found for the scanned barcode.`;
        return result;
      }

      const topConfidence = confidenceFromScore(top.score);
      result.match = {
        confidence: topConfidence,
        score: top.score,
        reasons: top.reasons,
        branchId: top.branch || null,
        isBranchOverlap: Boolean(branchScope && top.branch && String(top.branch).trim() !== branchScope),
        stock: top.row,
      };

      const row = top.row || {};
      const valuationInput = {
        make: firstNonEmpty([scanVehicle.make, row.make]),
        model: firstNonEmpty([scanVehicle.model, row.model]),
        variant: firstNonEmpty([scanVehicle.variant, row.variant]),
        year: normalizeYear(firstNonEmpty([scanVehicle.year, row.year])),
        mileage: firstNonEmpty([scanVehicle.mileage, row.mileageInKm]),
      };
      const hasValuationInput =
        Boolean(valuationInput.make) &&
        Boolean(valuationInput.model) &&
        Boolean(valuationInput.variant) &&
        Boolean(valuationInput.year) &&
        Boolean(String(valuationInput.mileage || "").trim());

      if (!hasValuationInput) {
        result.valuationWarning = "Matched stock does not contain complete valuation fields (make/model/variant/year/mileage).";
        return result;
      }

      try {
        const report = await fetchTruTradeReport(valuationInput);
        result.valuation = {
          tradePrice: report.tradePrice || report.truetrade_tradePrice || null,
          retailPrice: report.retailPrice || report.truetrade_retailPrice || null,
          marketPrice: report.marketPrice || report.truetrade_marketPrice || null,
        };
        result.tradePrice = result.valuation.tradePrice;
        result.retailPrice = result.valuation.retailPrice;
        result.marketPrice = result.valuation.marketPrice;
        result.valuationInput = valuationInput;
      } catch (e) {
        const mapped = mapValuationError(e);
        result.valuationError = {
          error: mapped.error,
          userMessage: mapped.userMessage,
          hint: mapped.hint || null,
          suggestions: mapped.suggestions || null,
        };
      }
      return result;
    }
    case VALUATION_COMMAND: {
      const make = payload?.make;
      const model = payload?.model;
      const variant = payload?.variant;
      const year = payload?.year;
      const mileage = payload?.mileage;

      const report = await fetchTruTradeReport({ make, model, variant, year, mileage });
      // ReportModel has tradePrice, retailPrice, marketPrice etc.
      result.report = report;
      result.valuation = {
        tradePrice: report.tradePrice || report.truetrade_tradePrice || null,
        retailPrice: report.retailPrice || report.truetrade_retailPrice || null,
        marketPrice: report.marketPrice || report.truetrade_marketPrice || null,
      };
      // Flatten key prices to top-level for convenience
      result.tradePrice = report.tradePrice || report.truetrade_tradePrice || null;
      result.retailPrice = report.retailPrice || report.truetrade_retailPrice || null;
      result.marketPrice = report.marketPrice || report.truetrade_marketPrice || null;
      result.avg_price = report.avg_price || null;
      result.high_price = report.high_price || null;
      result.low_price = report.low_price || null;
      result.avg_mileage = report.avg_mileage || null;
      return result;
    }
    default:
      throw new Error(`Unknown commandType: ${commandType}`);
  }
}

app.get("/", (req, res) => {
  res.type("application/json").json({
    ok: true,
    service: "cubeone-scan-connector",
    hint: "API routes live under /api/v1. Use GET /healthz for a quick up-check.",
    health: "/healthz",
    ready: "/readyz",
  });
});

app.get("/pilot/evolvesa", (req, res) => {
  return res.redirect(302, "/pilot/evolvesa/app-evolvesa-release.apk");
});

app.get("/pilot/evolvesa/app-evolvesa-release.apk", (req, res) => {
  const apkPath = path.join(STATIC_PUBLIC_DIR, "pilot", "evolvesa", "app-evolvesa-release.apk");
  if (!fs.existsSync(apkPath)) {
    return res.status(404).type("html").send(`<!doctype html>
<html lang="en"><head><meta charset="utf-8" /><meta name="viewport" content="width=device-width, initial-scale=1" />
<title>APK Not Uploaded</title></head><body style="font-family: Arial, sans-serif; padding: 24px;">
<h2>EvolveSA APK not uploaded yet</h2>
<p>Upload <code>app-evolvesa-release.apk</code> to <code>connector/public/pilot/evolvesa/</code> and redeploy the connector.</p>
</body></html>`);
  }
  return res.sendFile(apkPath);
});

app.get("/healthz", (req, res) => {
  res.json({ ok: true });
});

app.get("/readyz", (req, res) => {
  try {
    ensureDataDir();
    fs.accessSync(CONNECTOR_DATA_DIR, fs.constants.W_OK);
    return res.json({
      ok: true,
      storage: "writable",
      commandsLoaded: commands.size,
      deadLetters: deadLetters.length,
    });
  } catch (e) {
    return res.status(503).json({
      ok: false,
      error: "storage_not_writable",
      detail: e?.message || String(e),
    });
  }
});

app.get("/admin/tenant-config", (req, res) => {
  return res.sendFile(path.join(__dirname, "admin", "tenant-admin.html"));
});

app.get("/api/v1/admin/tenant-config", requireTenantAdmin, (req, res) => {
  return res.json({
    ok: true,
    ...tenantConfigSnapshot(),
  });
});

app.get("/api/v1/admin/tenant-config/history", requireTenantAdmin, (req, res) => {
  const limitRaw = Number(req.query.limit || 50);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, Math.floor(limitRaw))) : 50;
  const history = readTenantConfigHistory().slice(-limit).reverse();
  return res.json({ ok: true, count: history.length, history });
});

app.post("/api/v1/admin/tenant-config/preflight", requireTenantAdmin, (req, res) => {
  const payload = req.body || {};
  const validationError = validateTenantConfigPayload(payload);
  if (validationError) {
    return res.status(400).json({ ok: false, error: "invalid_tenant_config", detail: validationError });
  }
  const report = buildTenantConfigPreflightReport(payload);
  return res.json({ ok: true, preflight: report });
});

app.put("/api/v1/admin/tenant-config", requireTenantAdmin, (req, res) => {
  if (!TENANT_ADMIN_EDITOR_ROLES.has(req.tenantAdminRole)) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_admin_editor_required" });
  }
  const payload = req.body || {};
  const validationError = validateTenantConfigPayload(payload);
  if (validationError) return res.status(400).json({ error: "invalid_tenant_config", detail: validationError });

  const source = "admin_api_runtime";
  applyTenantConfigPayload(payload, source);

  const saveTarget = String(process.env.TENANT_CONFIG_FILE || "").trim() || TENANT_CONFIG_RUNTIME_FILE;
  try {
    ensureDataDir();
    fs.writeFileSync(saveTarget, JSON.stringify(payload, null, 2), "utf8");
    appendTenantConfigHistory("update", payload, req);
  } catch (e) {
    return res.status(500).json({
      error: "tenant_config_persist_failed",
      detail: e?.message || String(e),
      appliedInMemory: true,
    });
  }
  log("info", "tenant_config_updated", {
    source,
    saveTarget,
    mappedUsers: USER_EMAIL_DEALER_MAP.size,
    dealerAliases: DEALER_ID_ALIASES.size,
    vmgScopes: VMG_DEALER_SCOPES.size,
  });
  return res.json({
    ok: true,
    savedTo: saveTarget,
    ...tenantConfigSnapshot(),
  });
});

app.get("/api/v1/admin/tenant-config/proposals", requireTenantAdmin, (req, res) => {
  const items = readTenantConfigProposals().slice().reverse();
  return res.json({ ok: true, count: items.length, proposals: items });
});

app.post("/api/v1/admin/tenant-config/proposals", requireTenantAdmin, (req, res) => {
  if (!TENANT_ADMIN_EDITOR_ROLES.has(req.tenantAdminRole)) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_admin_editor_required" });
  }
  const payload = req.body || {};
  const validationError = validateTenantConfigPayload(payload);
  if (validationError) return res.status(400).json({ error: "invalid_tenant_config", detail: validationError });
  const proposals = readTenantConfigProposals();
  const proposal = {
    id: `prop_${uuidv4()}`,
    status: "pending",
    createdAt: new Date().toISOString(),
    createdBy: req.tenantAdminEmail || "unknown",
    role: req.tenantAdminRole || "unknown",
    payload,
  };
  proposals.push(proposal);
  try {
    ensureDataDir();
    writeTenantConfigProposals(proposals);
  } catch (e) {
    return res.status(500).json({ error: "proposal_persist_failed", detail: e?.message || String(e) });
  }
  return res.status(202).json({ ok: true, proposal });
});

app.post("/api/v1/admin/tenant-config/proposals/:id/approve", requireTenantAdmin, (req, res) => {
  if (!TENANT_ADMIN_APPROVER_ROLES.has(req.tenantAdminRole)) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_admin_approver_required" });
  }
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "id_required" });
  const proposals = readTenantConfigProposals();
  const idx = proposals.findIndex((x) => String(x?.id || "") === id);
  if (idx < 0) return res.status(404).json({ error: "proposal_not_found" });
  const proposal = proposals[idx];
  if (proposal.status !== "pending") return res.status(409).json({ error: "proposal_not_pending" });
  if (req.tenantAdminEmail && proposal.createdBy === req.tenantAdminEmail) {
    return res.status(403).json({ error: "forbidden", hint: "maker_cannot_approve_own_change" });
  }
  const validationError = validateTenantConfigPayload(proposal.payload);
  if (validationError) return res.status(400).json({ error: "invalid_tenant_config", detail: validationError });
  const source = "approved_proposal_runtime";
  applyTenantConfigPayload(proposal.payload, source);
  const saveTarget = String(process.env.TENANT_CONFIG_FILE || "").trim() || TENANT_CONFIG_RUNTIME_FILE;
  try {
    ensureDataDir();
    fs.writeFileSync(saveTarget, JSON.stringify(proposal.payload, null, 2), "utf8");
    proposal.status = "approved";
    proposal.approvedAt = new Date().toISOString();
    proposal.approvedBy = req.tenantAdminEmail || "unknown";
    proposals[idx] = proposal;
    writeTenantConfigProposals(proposals);
    appendTenantConfigHistory("proposal_approved", proposal.payload, req);
  } catch (e) {
    return res.status(500).json({ error: "tenant_config_persist_failed", detail: e?.message || String(e) });
  }
  return res.json({ ok: true, proposal, savedTo: saveTarget, ...tenantConfigSnapshot() });
});

app.post("/api/v1/admin/tenant-config/proposals/:id/reject", requireTenantAdmin, (req, res) => {
  if (!TENANT_ADMIN_APPROVER_ROLES.has(req.tenantAdminRole)) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_admin_approver_required" });
  }
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "id_required" });
  const proposals = readTenantConfigProposals();
  const idx = proposals.findIndex((x) => String(x?.id || "") === id);
  if (idx < 0) return res.status(404).json({ error: "proposal_not_found" });
  const proposal = proposals[idx];
  if (proposal.status !== "pending") return res.status(409).json({ error: "proposal_not_pending" });
  proposal.status = "rejected";
  proposal.rejectedAt = new Date().toISOString();
  proposal.rejectedBy = req.tenantAdminEmail || "unknown";
  proposals[idx] = proposal;
  try {
    ensureDataDir();
    writeTenantConfigProposals(proposals);
  } catch (e) {
    return res.status(500).json({ error: "proposal_persist_failed", detail: e?.message || String(e) });
  }
  return res.json({ ok: true, proposal });
});

app.post("/api/v1/admin/tenant-config/rollback/:id", requireTenantAdmin, (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ error: "id_required" });
  const history = readTenantConfigHistory();
  const hit = history.find((x) => String(x?.id || "") === id);
  if (!hit || !hit.payload || typeof hit.payload !== "object") {
    return res.status(404).json({ error: "history_entry_not_found" });
  }
  const validationError = validateTenantConfigPayload(hit.payload);
  if (validationError) {
    return res.status(400).json({ error: "invalid_history_payload", detail: validationError });
  }
  applyTenantConfigPayload(hit.payload, "rollback_runtime");
  const saveTarget = String(process.env.TENANT_CONFIG_FILE || "").trim() || TENANT_CONFIG_RUNTIME_FILE;
  try {
    ensureDataDir();
    fs.writeFileSync(saveTarget, JSON.stringify(hit.payload, null, 2), "utf8");
    appendTenantConfigHistory("rollback", hit.payload, req);
  } catch (e) {
    return res.status(500).json({
      error: "tenant_config_persist_failed",
      detail: e?.message || String(e),
      appliedInMemory: true,
    });
  }
  return res.json({
    ok: true,
    rolledBackTo: id,
    savedTo: saveTarget,
    ...tenantConfigSnapshot(),
  });
});

app.use((req, res, next) => {
  const requestId = `req_${uuidv4()}`;
  req.requestId = requestId;
  res.setHeader("x-request-id", requestId);
  const started = Date.now();
  res.on("finish", () => {
    log("info", "http_request", {
      requestId,
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      ms: Date.now() - started,
      ip: req.ip,
    });
  });
  next();
});

app.get(["/api/v1/valuation/lookup/:type", "/api/v1/trutrade/lookup/:type"], requireAuth, async (req, res) => {
  const authHeader = truTradeBasicAuthHeaderValue();
  if (!authHeader) {
    return res.status(503).json({
      error: "valuation_not_configured",
      hint: "Set TRUTRADE_API_KEY+TRUTRADE_API_SECRET or TRUTRADE_AUTH_USERNAME+TRUTRADE_AUTH_PASSWORD",
    });
  }

  const type = String(req.params.type || "").trim().toLowerCase();
  const make = String(req.query.make || "").trim();
  const model = String(req.query.model || "").trim();
  const year = String(req.query.year || "").trim();
  const base = (process.env.TRUTRADE_BASE_URL || "https://api.yourvehiclevalue.co.za/api").replace(/\/$/, "");

  let upstreamUrl = "";
  if (type === "make") upstreamUrl = `${base}/lookup/make`;
  if (type === "model") {
    if (!make) return res.status(400).json({ error: "make is required for model lookup" });
    upstreamUrl = `${base}/lookup/model/${encodeURIComponent(make)}`;
  }
  if (type === "year") {
    if (!make || !model) return res.status(400).json({ error: "make and model are required for year lookup" });
    upstreamUrl = `${base}/lookup/year/${encodeURIComponent(make)}/${encodeURIComponent(model)}`;
  }
  if (type === "variant") {
    if (!make || !model || !year) return res.status(400).json({ error: "make, model and year are required for variant lookup" });
    upstreamUrl = `${base}/lookup/variant/${encodeURIComponent(make)}/${encodeURIComponent(model)}/${encodeURIComponent(year)}`;
  }
  if (!upstreamUrl) {
    return res.status(400).json({ error: "unsupported lookup type (use: make|model|year|variant)" });
  }

  try {
    const upstream = await fetch(upstreamUrl, {
      method: "GET",
      headers: {
        Accept: "application/json",
        Authorization: authHeader,
      },
    });
    const text = await upstream.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch (_) { json = null; }
    if (!upstream.ok) {
      return res.status(upstream.status).json({
        error: "valuation_lookup_failed",
        type,
        upstreamStatus: upstream.status,
        detail: json || text || null,
      });
    }
    const values = Array.isArray(json) ? json : Array.isArray(json?.value) ? json.value : Array.isArray(json?.data) ? json.data : [];
    return res.json({
      ok: true,
      type,
      make: make || null,
      model: model || null,
      year: year || null,
      values,
      raw: json,
    });
  } catch (e) {
    return res.status(502).json({
      error: "valuation_lookup_exception",
      type,
      detail: e?.message || String(e),
    });
  }
});

app.get("/api/v1/stocks", requireAuth, extractTenantContext, async (req, res) => {
  try {
    const forceRefresh = String(req.query.refresh || "0").toLowerCase() === "1";
    const listMode = String(req.query.mode || "").trim().toLowerCase();
    const isStockTakeMode = listMode === "stock_take";
    const search = String(req.query.search || "").trim().toLowerCase();
    const limitRaw = Number(req.query.limit || 100);
    const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 100;
    const offsetRaw = Number(req.query.offset || 0);
    const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0;

    const dealerScope = String(req.tenantContext?.dealerId || "").trim();
    const branchScope = String(req.tenantContext?.branchId || "").trim();

    // Strict business isolation: never return stock without dealer scope.
    if (!dealerScope) {
      return res.json({
        ok: true,
        source: "empty_no_dealer_scope",
        mode: isStockTakeMode ? "stock_take" : "share_stock",
        cachedAt: null,
        total: 0,
        offset,
        limit,
        count: 0,
        dealerScope: null,
        branchScope: branchScope || null,
        stocks: [],
        warning: "Missing dealer scope; stock list withheld for business isolation.",
      });
    }

    const useVmgFeed = useVmgFeedForDealer(dealerScope);
    let fetched;
    try {
      fetched = useVmgFeed
        ? await fetchVmgListingsCached(forceRefresh, dealerScope)
        : await fetchAutoTraderListingsCached(forceRefresh);
    } catch (refreshErr) {
      if (!forceRefresh) throw refreshErr;
      // Upstream can intermittently fail on forced refresh.
      // Serve last known cache instead of hard-failing stock lookup in the app.
      fetched = useVmgFeed
        ? await fetchVmgListingsCached(false, dealerScope)
        : await fetchAutoTraderListingsCached(false);
      log("warn", "stock_refresh_failed_serving_cache", {
        error: refreshErr?.message || String(refreshErr),
        provider: useVmgFeed ? "vmg" : "autotrader",
      });
    }
    let rows = fetched.rows.slice();

    // VMG feeds are dedicated per dealer URL; AutoTrader needs explicit dealer filtering.
    if (!useVmgFeed) {
      rows = rows.filter((r) => stockMatchesDealerScope(r, dealerScope));
    }
    if (branchScope && !isStockTakeMode) {
      rows = rows.filter((r) => {
        const raw = r.raw || {};
        const stockBranch = String(raw.branchId || raw.branchCode || raw.branch || "").trim();
        return !stockBranch || stockBranch === branchScope;
      });
    }

    if (search) {
      rows = rows.filter((r) => {
        const hay = [
          r.stockNumber,
          r.registrationNumber,
          r.make,
          r.model,
          r.variant,
        ].join(" ").toLowerCase();
        return hay.includes(search);
      });
    }

    const total = rows.length;
    const pagedRaw = rows.slice(offset, offset + limit);
    const paged = isStockTakeMode ? await enrichStocksForStockTake(pagedRaw) : pagedRaw;

    return res.json({
      ok: true,
      source: fetched.source,
      provider: useVmgFeed ? "vmg" : "autotrader",
      mode: isStockTakeMode ? "stock_take" : "share_stock",
      cachedAt: fetched.cachedAt,
      total,
      offset,
      limit,
      count: paged.length,
      dealerScope: dealerScope || null,
      branchScope: branchScope || null,
      stocks: paged,
    });
  } catch (e) {
    // Last-resort fallback: if we have any cached rows in memory, serve them instead of 502.
    const dealerScopeForProvider = String(req.tenantContext?.dealerId || "").trim();
    const useVmgFeed = useVmgFeedForDealer(dealerScopeForProvider);
    const providerCacheRows = useVmgFeed
      ? vmgCachedRowsForDealerScope(dealerScopeForProvider)
      : autoTraderListingsCache.rows;
    const providerFetchedAtMs = useVmgFeed
      ? getVmgListingsBucket(resolveVmgStockFeedUrl(dealerScopeForProvider)).fetchedAtMs
      : autoTraderListingsCache.fetchedAtMs;
    if (Array.isArray(providerCacheRows) && providerCacheRows.length > 0) {
      const search = String(req.query.search || "").trim().toLowerCase();
      const limitRaw = Number(req.query.limit || 100);
      const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(500, Math.floor(limitRaw))) : 100;
      const offsetRaw = Number(req.query.offset || 0);
      const offset = Number.isFinite(offsetRaw) ? Math.max(0, Math.floor(offsetRaw)) : 0;
      const dealerScope = String(req.tenantContext?.dealerId || "").trim();
      const branchScope = String(req.tenantContext?.branchId || "").trim();

      let rows = providerCacheRows.slice();
      if (!dealerScope) {
        const listMode = String(req.query.mode || "").trim().toLowerCase();
        const isStockTakeMode = listMode === "stock_take";
        return res.json({
          ok: true,
          source: "empty_no_dealer_scope",
          mode: isStockTakeMode ? "stock_take" : "share_stock",
          cachedAt: null,
          total: 0,
          offset,
          limit,
          count: 0,
          dealerScope: null,
          branchScope: branchScope || null,
          stocks: [],
          warning: "Missing dealer scope; stock list withheld for business isolation.",
        });
      }
      if (!useVmgFeed) {
        rows = rows.filter((r) => stockMatchesDealerScope(r, dealerScope));
      }
      if (branchScope && !isStockTakeMode) {
        rows = rows.filter((r) => {
          const raw = r.raw || {};
          const stockBranch = String(raw.branchId || raw.branchCode || raw.branch || "").trim();
          return !stockBranch || stockBranch === branchScope;
        });
      }
      if (search) {
        rows = rows.filter((r) => {
          const hay = [
            r.stockNumber,
            r.registrationNumber,
            r.make,
            r.model,
            r.variant,
          ].join(" ").toLowerCase();
          return hay.includes(search);
        });
      }

      const total = rows.length;
      const listMode = String(req.query.mode || "").trim().toLowerCase();
      const isStockTakeMode = listMode === "stock_take";
      const pagedRaw = rows.slice(offset, offset + limit);
      const paged = isStockTakeMode ? await enrichStocksForStockTake(pagedRaw) : pagedRaw;
      log("warn", "stock_endpoint_served_stale_cache", {
        error: e?.message || String(e),
        total,
        count: paged.length,
      });
      return res.json({
        ok: true,
        source: "cache_stale_fallback",
        mode: isStockTakeMode ? "stock_take" : "share_stock",
        provider: useVmgFeed ? "vmg" : "autotrader",
        cachedAt: providerFetchedAtMs
          ? new Date(providerFetchedAtMs).toISOString()
          : null,
        total,
        offset,
        limit,
        count: paged.length,
        dealerScope: dealerScope || null,
        branchScope: branchScope || null,
        stocks: paged,
      });
    }

    log("error", "stock_list_fetch_failed_returning_empty", {
      error: e?.message || String(e),
    });
    return res.json({
      ok: true,
      source: "empty_fallback",
      mode: String(req.query.mode || "").trim().toLowerCase() === "stock_take" ? "stock_take" : "share_stock",
      cachedAt: null,
      total: 0,
      offset: 0,
      limit: 100,
      count: 0,
      dealerScope: String(req.tenantContext?.dealerId || "").trim() || null,
      branchScope: String(req.tenantContext?.branchId || "").trim() || null,
      stocks: [],
      warning: "Stock upstream temporarily unavailable; showing empty list fallback.",
      detail: e?.message || String(e),
    });
  }
});

app.post("/api/v1/stock-take/match", requireAuth, extractTenantContext, async (req, res) => {
  try {
    const payload = isPlainObject(req.body) ? req.body : {};
    const result = await processCommand("STOCK_TAKE", {
      ...payload,
      _tenantContext: req.tenantContext || null,
    });
    return res.json({
      ok: true,
      commandType: "STOCK_TAKE",
      result,
    });
  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
});

app.get("/api/v1/stock-take/items", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.query.sessionId || "").trim();
  if (!sessionId) {
    return sendApiError(req, res, 400, "invalid_session", "sessionId query param is required.");
  }
  const existing = stockTakeSessions.get(sessionId);
  if (!existing) {
    return res.json({
      ok: true,
      sessionId,
      status: "open",
      items: [],
      count: 0,
    });
  }
  if (!canAccessTenantScopedRecord(existing.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot access this stock take session.");
  }
  return res.json({
    ok: true,
    sessionId,
    status: existing.status,
    items: existing.items || [],
    count: Array.isArray(existing.items) ? existing.items.length : 0,
    createdAt: existing.createdAt || null,
    updatedAt: existing.updatedAt || null,
    submittedAt: existing.submittedAt || null,
  });
});

app.post("/api/v1/stock-take/items", requireAuth, extractTenantContext, async (req, res) => {
  try {
    const body = isPlainObject(req.body) ? req.body : {};
    const sessionId = String(body.sessionId || "").trim();
    if (!sessionId) {
      return sendApiError(req, res, 400, "invalid_session", "sessionId is required.");
    }
    const mode = String(body.mode || "").trim().toLowerCase();
    if (!["autotrader_list", "physical_scan", "manual_added"].includes(mode)) {
      return sendApiError(req, res, 400, "invalid_mode", "mode must be autotrader_list, physical_scan, or manual_added.");
    }
    const session = ensureStockTakeSession(sessionId, req.tenantContext || {});
    if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
      return sendApiError(req, res, 403, "forbidden", "You cannot modify this stock take session.");
    }
    if (session.status !== "open") {
      return sendApiError(req, res, 409, "session_closed", "Stock take session is already submitted.");
    }

    let item = null;
    if (mode === "autotrader_list") {
      const stock = body.stock;
      if (!isPlainObject(stock)) {
        return sendApiError(req, res, 400, "invalid_stock", "stock object is required for autotrader_list mode.");
      }
      item = buildStockTakeItemFromStock(stock, "autotrader_list", { notes: body.notes });
    } else {
      const payload = isPlainObject(body.payload) ? body.payload : {};
      const matchResult = await processCommand("STOCK_TAKE", {
        ...payload,
        _tenantContext: req.tenantContext || null,
      });
      const matchedStock = matchResult?.match?.stock || null;
      item = buildStockTakeItemFromStock(matchedStock, "physical_scan", {
        scan: payload?.scan || payload?.barcode || payload?.vehicle || payload || null,
        manual: mode === "manual_added" ? payload : null,
        notes: body.notes,
      });
      item.status = matchedStock ? "matched" : (mode === "manual_added" ? "manual_added" : "new_unmatched");
      item.match = matchResult?.match || null;
      item.possibleMatches = Array.isArray(matchResult?.possibleMatches) ? matchResult.possibleMatches : [];
      item.stockLookup = matchResult?.stockLookup || null;
      item.valuation = matchResult?.valuation || null;
      item.tradePrice = matchResult?.tradePrice ?? null;
      item.retailPrice = matchResult?.retailPrice ?? null;
      item.marketPrice = matchResult?.marketPrice ?? null;
      if (mode === "manual_added") item.source = "manual_added";
    }

    session.items.push(item);
    session.updatedAt = new Date().toISOString();
    stockTakeSessions.set(sessionId, session);
    persistStore();

    return res.status(201).json({
      ok: true,
      sessionId,
      item,
      count: session.items.length,
    });
  } catch (e) {
    return sendApiError(req, res, 500, "stock_take_item_create_failed", e?.message || String(e));
  }
});

app.post("/api/v1/stock-take/items/:itemId/link-stock", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.body?.sessionId || "").trim();
  const itemId = String(req.params.itemId || "").trim();
  if (!sessionId || !itemId) {
    return sendApiError(req, res, 400, "invalid_request", "sessionId and itemId are required.");
  }
  const session = stockTakeSessions.get(sessionId);
  if (!session) return sendApiError(req, res, 404, "session_not_found");
  if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot modify this stock take session.");
  }
  if (session.status !== "open") {
    return sendApiError(req, res, 409, "session_closed", "Stock take session is already submitted.");
  }
  const item = (session.items || []).find((x) => String(x.itemId) === itemId);
  if (!item) return sendApiError(req, res, 404, "item_not_found");
  const stock = req.body?.stock;
  if (!isPlainObject(stock)) {
    return sendApiError(req, res, 400, "invalid_stock", "stock object is required.");
  }
  item.linkedStock = stock;
  item.stock = stock;
  item.status = "matched";
  item.pricing = {
    autoTraderPrice: stock?.autoTraderPrice ?? stock?.price ?? null,
    tradePrice: stock?.tradePrice ?? item?.tradePrice ?? null,
    retailPrice: stock?.retailPrice ?? item?.retailPrice ?? null,
    marketPrice: stock?.marketPrice ?? item?.marketPrice ?? null,
    valuationStatus: stock?.valuationStatus || item?.pricing?.valuationStatus || null,
  };
  item.updatedAt = new Date().toISOString();
  session.updatedAt = new Date().toISOString();
  persistStore();
  return res.json({ ok: true, sessionId, item });
});

app.delete("/api/v1/stock-take/items/:itemId", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.query.sessionId || req.body?.sessionId || "").trim();
  const itemId = String(req.params.itemId || "").trim();
  if (!sessionId || !itemId) {
    return sendApiError(req, res, 400, "invalid_request", "sessionId and itemId are required.");
  }
  const session = stockTakeSessions.get(sessionId);
  if (!session) return sendApiError(req, res, 404, "session_not_found");
  if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot modify this stock take session.");
  }
  if (session.status !== "open") {
    return sendApiError(req, res, 409, "session_closed", "Stock take session is already submitted.");
  }
  const before = Array.isArray(session.items) ? session.items.length : 0;
  session.items = (session.items || []).filter((x) => String(x.itemId) !== itemId);
  if (session.items.length === before) return sendApiError(req, res, 404, "item_not_found");
  session.updatedAt = new Date().toISOString();
  persistStore();
  return res.json({ ok: true, sessionId, count: session.items.length });
});

app.post("/api/v1/stock-take/submit", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.body?.sessionId || "").trim();
  if (!sessionId) return sendApiError(req, res, 400, "invalid_session", "sessionId is required.");
  const session = stockTakeSessions.get(sessionId);
  if (!session) return sendApiError(req, res, 404, "session_not_found");
  if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot submit this stock take session.");
  }
  if (session.status !== "open") {
    return sendApiError(req, res, 409, "session_closed", "Stock take session is already submitted.");
  }

  const items = Array.isArray(session.items) ? session.items : [];
  const summary = {
    totalItems: items.length,
    matched: items.filter((x) => x.status === "matched").length,
    newUnmatched: items.filter((x) => x.status === "new_unmatched").length,
    manualAdded: items.filter((x) => x.status === "manual_added").length,
  };
  session.status = "submitted";
  session.submittedAt = new Date().toISOString();
  session.updatedAt = session.submittedAt;
  session.submittedSummary = summary;
  persistStore();
  return res.json({
    ok: true,
    sessionId,
    status: session.status,
    submittedAt: session.submittedAt,
    summary,
    items,
  });
});

app.get("/api/v1/analytics/kpis", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const dealerId = String(req.query.dealerId || tenant.dealerId || "").trim();
  const branchId = String(req.query.branchId || tenant.branchId || "").trim();
  const from = toIsoDate(req.query.from || `${monthPeriod()}-01T00:00:00.000Z`);
  const to = toIsoDate(req.query.to || new Date());
  if (!dealerId) return sendApiError(req, res, 400, "invalid_dealer", "dealerId is required.");
  const rows = commandsInWindow(from, to);
  const kpis = aggregateKpis(rows, dealerId, branchId);
  return res.json({
    ok: true,
    dealerId,
    branchId: branchId || null,
    from,
    to,
    kpis,
  });
});

app.put("/api/v1/analytics/targets", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const body = isPlainObject(req.body) ? req.body : {};
  const dealerId = String(body.dealerId || tenant.dealerId || "").trim();
  const branchId = String(body.branchId || "").trim();
  const period = String(body.period || monthPeriod()).trim();
  if (!dealerId) return sendApiError(req, res, 400, "invalid_dealer", "dealerId is required.");
  if (!/^\d{4}-\d{2}$/.test(period)) return sendApiError(req, res, 400, "invalid_period", "period must be YYYY-MM.");
  const targets = isPlainObject(body.targets) ? body.targets : {};
  const normalized = {
    leads: Number(targets.leads || 0),
    stockUnits: Number(targets.stockUnits || 0),
    shares: Number(targets.shares || 0),
    creditChecks: Number(targets.creditChecks || 0),
    deliveries: Number(targets.deliveries || 0),
    testDrives: Number(targets.testDrives || 0),
  };
  const plan = {
    dealerId,
    branchId,
    period,
    targets: normalized,
    updatedAt: new Date().toISOString(),
    updatedByUserId: tenant.userId || null,
    updatedByEmail: tenant.userEmail || null,
  };
  targetPlans.set(tenantKey(dealerId, branchId, period), plan);
  persistStore();
  return res.json({ ok: true, plan });
});

app.get("/api/v1/analytics/targets", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const dealerId = String(req.query.dealerId || tenant.dealerId || "").trim();
  const branchId = String(req.query.branchId || "").trim();
  const period = String(req.query.period || monthPeriod()).trim();
  if (!dealerId) return sendApiError(req, res, 400, "invalid_dealer", "dealerId is required.");
  const plan = targetPlans.get(tenantKey(dealerId, branchId, period)) || null;
  return res.json({ ok: true, dealerId, branchId: branchId || null, period, plan });
});

app.get("/api/v1/analytics/forecast", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const dealerId = String(req.query.dealerId || tenant.dealerId || "").trim();
  const branchId = String(req.query.branchId || "").trim();
  const period = String(req.query.period || monthPeriod()).trim();
  if (!dealerId) return sendApiError(req, res, 400, "invalid_dealer", "dealerId is required.");
  const from = `${period}-01T00:00:00.000Z`;
  const to = new Date().toISOString();
  const rows = commandsInWindow(from, to);
  const kpis = aggregateKpis(rows, dealerId, branchId);
  const now = new Date();
  const start = new Date(`${period}-01T00:00:00.000Z`);
  const elapsedDays = Math.max(1, Math.ceil((now.getTime() - start.getTime()) / 86400000));
  const forecast = {
    leads: rollingForecast(kpis.createdLeads, elapsedDays, 30),
    stockUnits: rollingForecast(kpis.stockUnits, elapsedDays, 30),
    shares: rollingForecast(kpis.sharedStock, elapsedDays, 30),
    creditChecks: rollingForecast(kpis.creditChecks, elapsedDays, 30),
  };
  const plan = targetPlans.get(tenantKey(dealerId, branchId, period)) || null;
  const target = plan?.targets || {};
  const alerts = [
    {
      metric: "leads",
      actual: kpis.createdLeads,
      forecast: forecast.leads,
      target: Number(target.leads || 0),
      band: computeBandLabel(forecast.leads ?? kpis.createdLeads, target.leads),
    },
    {
      metric: "stockUnits",
      actual: kpis.stockUnits,
      forecast: forecast.stockUnits,
      target: Number(target.stockUnits || 0),
      band: computeBandLabel(forecast.stockUnits ?? kpis.stockUnits, target.stockUnits),
    },
    {
      metric: "shares",
      actual: kpis.sharedStock,
      forecast: forecast.shares,
      target: Number(target.shares || 0),
      band: computeBandLabel(forecast.shares ?? kpis.sharedStock, target.shares),
    },
  ];
  return res.json({ ok: true, dealerId, branchId: branchId || null, period, elapsedDays, kpis, forecast, alerts });
});

app.get("/api/v1/analytics/oem-rollup", requireAuth, extractTenantContext, (req, res) => {
  const period = String(req.query.period || monthPeriod()).trim();
  const from = `${period}-01T00:00:00.000Z`;
  const to = new Date().toISOString();
  const rows = commandsInWindow(from, to);
  const dealerIds = new Set();
  for (const c of rows) {
    const d = String(c?.tenantContext?.dealerId || "").trim();
    if (d) dealerIds.add(d);
  }
  const dealers = Array.from(dealerIds).map((dealerId) => {
    const kpis = aggregateKpis(rows, dealerId, "");
    const plan = targetPlans.get(tenantKey(dealerId, "", period)) || null;
    return {
      dealerId,
      kpis,
      targetSummary: plan?.targets || null,
      leadBand: computeBandLabel(kpis.createdLeads, plan?.targets?.leads),
    };
  });
  const totals = dealers.reduce(
    (acc, d) => {
      acc.createdLeads += Number(d.kpis.createdLeads || 0);
      acc.stockUnits += Number(d.kpis.stockUnits || 0);
      acc.sharedStock += Number(d.kpis.sharedStock || 0);
      acc.creditChecks += Number(d.kpis.creditChecks || 0);
      return acc;
    },
    { createdLeads: 0, stockUnits: 0, sharedStock: 0, creditChecks: 0 }
  );
  return res.json({ ok: true, period, dealers, totals });
});

app.post("/api/v1/test-drives/start", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const body = isPlainObject(req.body) ? req.body : {};
  const leadId = String(body.leadId || "").trim();
  const vehicleRef = String(body.vehicleRef || body.stockUnitId || "").trim();
  const driverIdNumber = String(body.driverIdNumber || "").trim();
  const mobile = String(body.mobile || "").trim();
  const emergencyMobile = String(body.emergencyMobile || "").trim();
  const plannedReturnAt = toIsoDate(body.plannedReturnAt);
  if (!leadId || !vehicleRef || !driverIdNumber || !mobile || !plannedReturnAt) {
    return sendApiError(
      req,
      res,
      400,
      "invalid_payload",
      "leadId, vehicleRef, driverIdNumber, mobile and plannedReturnAt are required."
    );
  }
  const sessionId = `td_${uuidv4()}`;
  const session = {
    sessionId,
    status: "active",
    startedAt: new Date().toISOString(),
    plannedReturnAt,
    checkedInAt: null,
    completedAt: null,
    tenantContext: buildTenantScope(tenant),
    salespersonUserId: tenant.userId || null,
    salespersonEmail: tenant.userEmail || null,
    leadId,
    vehicleRef,
    driverIdNumber,
    mobile,
    emergencyMobile: emergencyMobile || null,
    currentLocation: body.currentLocation || null,
    notes: String(body.notes || "").trim() || null,
    checkins: [],
  };
  testDriveSessions.set(sessionId, session);
  persistStore();
  return res.status(201).json({ ok: true, session });
});

app.post("/api/v1/test-drives/:sessionId/checkin", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.params.sessionId || "").trim();
  const session = testDriveSessions.get(sessionId);
  if (!session) return sendApiError(req, res, 404, "not_found");
  if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot check in this session.");
  }
  if (session.status !== "active") {
    return sendApiError(req, res, 409, "invalid_state", "Only active sessions can be checked in.");
  }
  const body = isPlainObject(req.body) ? req.body : {};
  const checkin = {
    at: new Date().toISOString(),
    location: body.location || null,
    note: String(body.note || "").trim() || null,
    userId: req.tenantContext?.userId || null,
  };
  session.checkins.push(checkin);
  session.checkedInAt = checkin.at;
  session.updatedAt = checkin.at;
  testDriveSessions.set(sessionId, session);
  persistStore();
  return res.json({ ok: true, sessionId, checkin });
});

app.post("/api/v1/test-drives/:sessionId/complete", requireAuth, extractTenantContext, (req, res) => {
  const sessionId = String(req.params.sessionId || "").trim();
  const session = testDriveSessions.get(sessionId);
  if (!session) return sendApiError(req, res, 404, "not_found");
  if (!canAccessTenantScopedRecord(session.tenantContext, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "You cannot complete this session.");
  }
  if (session.status !== "active") {
    return sendApiError(req, res, 409, "invalid_state", "Session is already closed.");
  }
  const body = isPlainObject(req.body) ? req.body : {};
  session.status = "completed";
  session.completedAt = new Date().toISOString();
  session.completedByUserId = req.tenantContext?.userId || null;
  session.returnLocation = body.returnLocation || null;
  session.returnNotes = String(body.returnNotes || "").trim() || null;
  session.updatedAt = session.completedAt;
  testDriveSessions.set(sessionId, session);
  persistStore();
  return res.json({ ok: true, session });
});

app.get("/api/v1/test-drives/active", requireAuth, extractTenantContext, (req, res) => {
  const tenant = req.tenantContext || {};
  const nowMs = Date.now();
  const sessions = Array.from(testDriveSessions.values()).filter((s) =>
    s.status === "active" && canAccessTenantScopedRecord(s.tenantContext, tenant)
  );
  const withRisk = sessions.map((s) => {
    const dueMs = Date.parse(String(s.plannedReturnAt || ""));
    const overdueMinutes = Number.isFinite(dueMs) ? Math.max(0, Math.floor((nowMs - dueMs) / 60000)) : 0;
    return {
      ...s,
      overdueMinutes,
      safetyStatus: overdueMinutes >= 30 ? "overdue_high_risk" : overdueMinutes > 0 ? "overdue" : "on_track",
    };
  });
  return res.json({ ok: true, count: withRisk.length, sessions: withRisk });
});

app.get("/api/v1/approvals", requireAuth, extractTenantContext, (req, res) => {
  const role = normalizeRole(req.tenantContext?.role);
  if (!isManagerOrPrincipal(role)) {
    return res.status(403).json({ error: "forbidden", hint: "manager_or_principal_required" });
  }
  const markSeen = String(req.query.markSeen || "1").toLowerCase() !== "0";
  const statusFilter = String(req.query.status || "pending").toLowerCase();
  const candidates = Array.from(commands.values())
    .filter((c) => Object.keys(REQUEST_TO_COMMAND).includes(String(c.commandType || "")))
    .filter((c) => statusFilter === "all" ? true : String(c.status || "").toLowerCase() === "pending_manager_approval")
    .filter((c) => {
      const dealerOk = !req.tenantContext?.dealerId || !c.tenantContext?.dealerId || c.tenantContext.dealerId === req.tenantContext.dealerId;
      const branchOk = !req.tenantContext?.branchId || !c.tenantContext?.branchId || c.tenantContext.branchId === req.tenantContext.branchId;
      return dealerOk && branchOk;
    })
    .sort((a, b) => String(a.createdAt || "").localeCompare(String(b.createdAt || "")));
  if (markSeen) {
    let changed = false;
    for (const c of candidates) {
      if (!c.managerSeenAt) {
        c.managerSeenAt = new Date().toISOString();
        c.managerSeenByUserId = req.tenantContext?.userId || null;
        c.updatedAt = new Date().toISOString();
        commands.set(c.correlationId, c);
        changed = true;
      }
    }
    if (changed) persistStore();
  }
  const rows = candidates
    .map((c) => ({
      correlationId: c.correlationId,
      status: c.status,
      commandType: c.commandType,
      requestedCommandType: REQUEST_TO_COMMAND[c.commandType] || null,
      createdAt: c.createdAt,
      managerSeen: Boolean(c.managerSeenAt),
      managerSeenAt: c.managerSeenAt || null,
      managerSeenByUserId: c.managerSeenByUserId || null,
      payload: c.payload,
      tenantContext: c.tenantContext || null,
      error: c.error || null,
    }));
  return res.json({ approvals: rows, count: rows.length });
});

app.post("/api/v1/approvals/:correlationId/approve", requireAuth, extractTenantContext, async (req, res) => {
  const role = normalizeRole(req.tenantContext?.role);
  if (!isManagerOrPrincipal(role)) {
    return res.status(403).json({ error: "forbidden", hint: "manager_or_principal_required" });
  }
  const correlationId = String(req.params.correlationId || "").trim();
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
  if (!REQUEST_TO_COMMAND[record.commandType]) return res.status(400).json({ error: "not_approval_request" });
  if (record.status !== "pending_manager_approval") return res.status(409).json({ error: "not_pending", status: record.status });

  const targetType = REQUEST_TO_COMMAND[record.commandType];
  try {
    record.status = "processing";
    record.updatedAt = new Date().toISOString();
    createOrUpdateCommand(record);

    const result = await processCommand(targetType, {
      ...(record.payload || {}),
      _tenantContext: record.tenantContext || null,
      _approval: {
        approvedByUserId: req.tenantContext?.userId || null,
        approvedByRole: role,
        approvedAt: new Date().toISOString(),
      },
    });
    record.status = "done";
    record.updatedAt = new Date().toISOString();
    record.result = {
      approved: true,
      approvedByUserId: req.tenantContext?.userId || null,
      approvedByRole: role,
      executedCommandType: targetType,
      output: result,
    };
    record.error = null;
    createOrUpdateCommand(record);
    return res.json({ ok: true, correlationId, status: record.status, result: record.result });
  } catch (e) {
    record.status = "failed";
    record.updatedAt = new Date().toISOString();
    record.error = e?.message || String(e);
    createOrUpdateCommand(record);
    return res.status(500).json({ error: "approval_execution_failed", detail: record.error });
  }
});

app.post("/api/v1/approvals/:correlationId/reject", requireAuth, extractTenantContext, (req, res) => {
  const role = normalizeRole(req.tenantContext?.role);
  if (!isManagerOrPrincipal(role)) {
    return res.status(403).json({ error: "forbidden", hint: "manager_or_principal_required" });
  }
  const correlationId = String(req.params.correlationId || "").trim();
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
  if (!REQUEST_TO_COMMAND[record.commandType]) return res.status(400).json({ error: "not_approval_request" });
  if (record.status !== "pending_manager_approval") return res.status(409).json({ error: "not_pending", status: record.status });

  const reason = String(req.body?.reason || "Rejected by manager").trim();
  record.status = "rejected";
  record.updatedAt = new Date().toISOString();
  record.error = reason;
  record.result = {
    approved: false,
    rejectedByUserId: req.tenantContext?.userId || null,
    rejectedByRole: role,
    rejectedAt: new Date().toISOString(),
    reason,
  };
  createOrUpdateCommand(record);
  return res.json({ ok: true, correlationId, status: record.status });
});

app.post("/api/v1/consents", requireAuth, extractTenantContext, async (req, res) => {
  try {
    const body = isPlainObject(req.body) ? req.body : {};
    const purpose = String(body.purpose || "").trim() || "soft_credit_check_affordability";
    const channel = String(body.channel || "").trim().toLowerCase() || "email_link";
    const noticeVersion = String(body.noticeVersion || "").trim();
    const leadCorrelationId = String(body.leadCorrelationId || "").trim();
    const leadId = String(body.leadId || "").trim();
    const applicant = isPlainObject(body.applicant) ? body.applicant : {};
    const email = normalizeEmail(applicant.email);
    const mobile = String(applicant.mobile || "").trim();
    const idNumber = String(applicant.idNumber || "").replace(/\s+/g, "");
    const firstName = String(applicant.firstName || "").trim();
    const surname = String(applicant.surname || applicant.lastName || "").trim();
    const expiresInHoursRaw = Number(body.expiresInHours || CONSENT_DEFAULT_EXPIRY_HOURS);
    const expiresInHours = Number.isFinite(expiresInHoursRaw)
      ? Math.max(1, Math.min(168, Math.floor(expiresInHoursRaw)))
      : CONSENT_DEFAULT_EXPIRY_HOURS;

    if (!noticeVersion) {
      return sendApiError(req, res, 400, "invalid_payload", "noticeVersion is required.");
    }
    if (!leadCorrelationId && !leadId) {
      return sendApiError(req, res, 400, "invalid_payload", "leadCorrelationId or leadId is required.");
    }
    if (!email || !email.includes("@")) {
      return sendApiError(req, res, 400, "invalid_payload", "applicant.email is required for email_link consent.");
    }
    if (!mobile || !idNumber) {
      return sendApiError(req, res, 400, "invalid_payload", "applicant.mobile and applicant.idNumber are required.");
    }
    if (channel !== "email_link") {
      return sendApiError(req, res, 400, "invalid_payload", "Only channel=email_link is supported currently.");
    }
    if (!String(req.tenantContext?.dealerId || "").trim()) {
      return sendApiError(req, res, 403, "tenant_scope_missing", "Dealer scope is required to create consent.");
    }

    for (const existing of consentRecords.values()) {
      if (String(existing.status || "").toLowerCase() !== "pending") continue;
      if (!canAccessTenantScopedRecord(existing.tenantContext || {}, req.tenantContext || {})) continue;
      // Allow a new request after a previous SMTP failure (still "pending" but not deliverable).
      if (String(existing.delivery?.emailDispatch || "") === "failed") continue;
      const existingLead = String(existing.leadCorrelationId || existing.leadId || "").trim();
      const incomingLead = String(leadCorrelationId || leadId).trim();
      if (existingLead && incomingLead && existingLead === incomingLead && existing.purpose === purpose) {
        // Idempotent "send again": mobile clients may lose in-memory state; return the open request instead of 409.
        log("info", "consent_create_reused_pending", {
          consentId: existing.consentId,
          leadCorrelationId: existing.leadCorrelationId || null,
          leadId: existing.leadId || null,
          dealerId: String(req.tenantContext?.dealerId || "").trim() || null,
        });
        return res.status(200).json({
          ok: true,
          reusedPendingConsent: true,
          ...consentPublicView(existing),
        });
      }
    }

    const now = Date.now();
    const requestedAt = new Date(now).toISOString();
    const expiresAt = new Date(now + expiresInHours * 60 * 60 * 1000).toISOString();
    const consentId = `consent_${uuidv4()}`;
    const tokenPayload = {
      cid: consentId,
      dealerId: String(req.tenantContext?.dealerId || "").trim(),
      purpose,
      nv: noticeVersion,
      jti: `ct_${uuidv4()}`,
      exp: Math.floor(Date.parse(expiresAt) / 1000),
    };
    const token = signConsentToken(tokenPayload);
    const approveUrl = buildConsentApprovalLink(req, token);

    // Respond before SMTP completes. Proxies (e.g. Render ~30s) often time out while Brevo is still sending.
    const record = {
      consentId,
      status: "pending",
      purpose,
      noticeVersion,
      requestedAt,
      expiresAt,
      approvedAt: null,
      rejectedAt: null,
      revokedAt: null,
      usedAt: null,
      leadCorrelationId: leadCorrelationId || null,
      leadId: leadId || null,
      approvalChannel: channel,
      applicant: {
        firstName: firstName || null,
        surname: surname || null,
        idNumberMasked: maskIdNumber(idNumber),
        email,
        mobile,
      },
      tenantContext: {
        dealerId: String(req.tenantContext?.dealerId || "").trim(),
        branchId: String(req.tenantContext?.branchId || "").trim(),
        userId: req.tenantContext?.userId || null,
      },
      requestedBy: {
        userId: req.tenantContext?.userId || null,
        userEmail: req.tenantContext?.userEmail || null,
        userName: req.tenantContext?.userName || null,
        role: req.tenantContext?.role || null,
      },
      requestMeta: {
        ip: req.ip || null,
        userAgent: req.headers["user-agent"] || null,
      },
      delivery: {
        emailSent: false,
        emailDispatch: "pending",
        providerRef: null,
        approveUrl,
      },
      tokenIssuedAt: requestedAt,
      updatedAt: requestedAt,
    };

    consentRecords.set(consentId, record);
    appendConsentEvent(consentId, "created", {
      dealerId: record.tenantContext.dealerId,
      leadCorrelationId: record.leadCorrelationId,
      leadId: record.leadId,
      purpose,
    });
    persistStore();

    log("info", "consent_create_accepted", {
      consentId,
      leadCorrelationId: leadCorrelationId || null,
      leadId: leadId || null,
      toDomain: email.includes("@") ? email.split("@").pop() : null,
      dealerId: String(req.tenantContext?.dealerId || "").trim() || null,
    });

    res.status(201).json({
      ok: true,
      ...consentPublicView(record),
    });

    const mailPayload = {
      consentId,
      purpose,
      applicant: {
        email,
        firstName,
        surname,
        idNumberMasked: maskIdNumber(idNumber),
      },
      tenantContext: req.tenantContext || {},
      expiresAt,
    };
    // Defer SMTP so the HTTP response can flush first (Render/proxy timeouts).
    log("info", "consent_email_dispatch_queued", { consentId });
    setTimeout(() => {
      void (async () => {
        log("info", "consent_email_dispatch_running", { consentId });
        try {
          const delivery = await notifyConsentEmail(mailPayload, approveUrl);
          const r = consentRecords.get(consentId);
          if (!r) return;
          const dispatch = delivery.emailSent ? "sent" : "failed";
          r.delivery = {
            ...delivery,
            emailDispatch: dispatch,
            approveUrl: r.delivery?.approveUrl || approveUrl,
          };
          r.updatedAt = new Date().toISOString();
          if (delivery.emailSent) {
            appendConsentEvent(consentId, "delivery_sent", {
              channel,
              email,
              providerRef: delivery.providerRef,
            });
          } else {
            appendConsentEvent(consentId, "delivery_failed", {
              channel,
              email,
              warning: delivery.warning || "unknown",
            });
          }
          consentRecords.set(consentId, r);
          persistStore();
        } catch (e) {
          log("error", "consent_email_async_failed", { consentId, error: e?.message || String(e) });
          const r = consentRecords.get(consentId);
          if (!r) return;
          const warn = e?.message || String(e);
          r.delivery = {
            ...(r.delivery || {}),
            emailSent: false,
            emailDispatch: "failed",
            warning: warn,
            approveUrl: r.delivery?.approveUrl || approveUrl,
          };
          r.updatedAt = new Date().toISOString();
          appendConsentEvent(consentId, "delivery_failed", {
            channel,
            email,
            warning: warn,
          });
          consentRecords.set(consentId, r);
          persistStore();
        }
      })();
    }, 0);
  } catch (e) {
    return sendApiError(req, res, 500, "consent_create_failed", e?.message || String(e));
  }
});

app.get("/api/v1/consents/:consentId", requireAuth, extractTenantContext, (req, res) => {
  const consentId = String(req.params.consentId || "").trim();
  const record = consentRecords.get(consentId);
  if (!record) {
    return sendApiError(req, res, 404, "consent_not_found", "Consent was not found.");
  }
  if (!canAccessTenantScopedRecord(record.tenantContext || {}, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "Consent does not belong to current tenant scope.");
  }
  return res.json({
    ok: true,
    ...consentPublicView(record),
    audit: consentAuditPayload(record),
  });
});

app.post("/api/v1/consents/:consentId/revoke", requireAuth, extractTenantContext, (req, res) => {
  const consentId = String(req.params.consentId || "").trim();
  const record = consentRecords.get(consentId);
  if (!record) {
    return sendApiError(req, res, 404, "consent_not_found", "Consent was not found.");
  }
  if (!canAccessTenantScopedRecord(record.tenantContext || {}, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "Consent does not belong to current tenant scope.");
  }
  const status = String(record.status || "").toLowerCase();
  if (status === "revoked") {
    return res.json({ ok: true, ...consentPublicView(record) });
  }
  if (status !== "pending" && status !== "approved") {
    return sendApiError(req, res, 409, "consent_not_revocable", `Consent in state ${record.status} cannot be revoked.`);
  }
  const nowIso = new Date().toISOString();
  record.status = "revoked";
  record.revokedAt = nowIso;
  record.updatedAt = nowIso;
  record.revokeReason = String(req.body?.reason || "").trim() || null;
  consentRecords.set(consentId, record);
  appendConsentEvent(consentId, "revoked", {
    byUserId: req.tenantContext?.userId || null,
    byUserEmail: req.tenantContext?.userEmail || null,
    reason: record.revokeReason,
  });
  persistStore();
  return res.json({ ok: true, ...consentPublicView(record) });
});

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Public form posts are urlencoded; send HTML unless the client explicitly wants JSON only (API / curl). */
function wantsConsentApproveHtml(req) {
  const ct = String(req.headers["content-type"] || "").toLowerCase();
  if (!ct.includes("application/x-www-form-urlencoded")) return false;
  const accept = String(req.headers.accept || "").trim();
  const a = accept.toLowerCase();
  if (!a || a.includes("text/html") || a.includes("application/xhtml+xml") || a.includes("*/*")) {
    return true;
  }
  const jsonOnly = /\bapplication\/json\b/i.test(accept) && !/\btext\/html\b/i.test(a);
  return !jsonOnly;
}

function setConsentApproveNoStore(res) {
  res.set("Cache-Control", "no-store, no-cache, must-revalidate, private");
  res.set("Pragma", "no-cache");
  res.set("Expires", "0");
}

function sendConsentApprovePage(res, httpStatus, title, innerHtml) {
  setConsentApproveNoStore(res);
  res
    .status(httpStatus)
    .type("html")
    .send(
      `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>${escapeHtml(title)}</title>` +
        `<meta name="viewport" content="width=device-width, initial-scale=1">` +
        `<style>body{font-family:system-ui,-apple-system,sans-serif;max-width:28rem;margin:2rem auto;padding:0 1rem;line-height:1.5;color:#111}` +
        `h1{font-size:1.25rem}p{color:#444}</style></head><body>` +
        `<h1>${escapeHtml(title)}</h1>${innerHtml}</body></html>`
    );
}

const consentApproveRouter = express.Router();
consentApproveRouter.use((req, res, next) => {
  setConsentApproveNoStore(res);
  next();
});
app.use("/consent/approve", consentApproveRouter);

consentApproveRouter.get("/", (req, res) => {
  const token = String(req.query.token || "").trim();
  if (!token) {
    return res.status(400).send("Missing token.");
  }
  try {
    const decoded = verifyConsentToken(token);
    const consentId = String(decoded?.cid || "").trim();
    const record = consentRecords.get(consentId);
    if (!record) return res.status(404).send("Consent not found.");
    const nowMs = Date.now();
    if (record.expiresAt && Date.parse(record.expiresAt) <= nowMs) {
      return res.status(410).send("Consent link has expired.");
    }
    const st = String(record.status || "").toLowerCase();
    if (st === "approved") {
      return sendConsentApprovePage(
        res,
        200,
        "Already approved",
        `<p>Your approval for this soft credit-check request is already on record.</p>` +
          `<p>Reference: <strong>${escapeHtml(consentId)}</strong></p>` +
          `<p>You can close this page.</p>`
      );
    }
    if (st === "rejected") {
      return sendConsentApprovePage(
        res,
        200,
        "Already declined",
        `<p>You have already declined this credit-check request.</p>` +
          `<p>Reference: <strong>${escapeHtml(consentId)}</strong></p>` +
          `<p>You can close this page.</p>`
      );
    }
    if (st === "expired" || st === "revoked") {
      return sendConsentApprovePage(
        res,
        410,
        "No longer active",
        `<p>This consent request is no longer active (status: <strong>${escapeHtml(record.status)}</strong>). ` +
          `Please contact the dealership if you still need assistance.</p>`
      );
    }
    const tokenEsc = escapeHtml(token);
    return res.status(200).send(
      `<!DOCTYPE html><html lang="en"><head><meta charset="utf-8"><title>Credit consent</title>` +
        `<meta name="viewport" content="width=device-width, initial-scale=1">` +
        `<style>body{font-family:system-ui,-apple-system,sans-serif;max-width:28rem;margin:2rem auto;padding:0 1rem;line-height:1.5}` +
        `button{margin-right:0.5rem;margin-top:0.5rem;padding:0.5rem 0.85rem;font-size:1rem}</style></head><body>` +
        `<h1>Credit consent</h1><p><strong>${escapeHtml(consentId)}</strong> — review and respond below.</p>` +
        `<form method="post" action="/consent/approve">` +
        `<input type="hidden" name="token" value="${tokenEsc}" />` +
        `<button type="submit" name="decision" value="approve">Approve</button>` +
        `<button type="submit" name="decision" value="reject">Reject</button>` +
        `</form></body></html>`
    );
  } catch (e) {
    return res.status(400).send("Invalid or expired token.");
  }
});

consentApproveRouter.post("/", (req, res) => {
  const html = wantsConsentApproveHtml(req);
  try {
    const token = String(req.body?.token || "").trim();
    const decision = String(req.body?.decision || "").trim().toLowerCase();
    if (!token) {
      if (html) {
        return sendConsentApprovePage(res, 400, "Consent link error", `<p>${escapeHtml("Missing token.")}</p>`);
      }
      return res.status(400).json({ ok: false, error: "invalid_token" });
    }
    if (decision !== "approve" && decision !== "reject") {
      if (html) {
        return sendConsentApprovePage(
          res,
          400,
          "Consent link error",
          `<p>${escapeHtml("Choose Approve or Reject.")}</p>`
        );
      }
      return res.status(400).json({ ok: false, error: "invalid_decision" });
    }
    const decoded = verifyConsentToken(token);
    const consentId = String(decoded?.cid || "").trim();
    const record = consentRecords.get(consentId);
    if (!record) {
      if (html) {
        return sendConsentApprovePage(res, 404, "Consent not found", `<p>This consent request is not available.</p>`);
      }
      return res.status(404).json({ ok: false, error: "consent_not_found" });
    }
    const status = String(record.status || "").toLowerCase();
    if (status !== "pending") {
      const sameOutcome =
        (decision === "approve" && status === "approved") ||
        (decision === "reject" && status === "rejected");
      if (sameOutcome) {
        const view = consentPublicView(record);
        if (html) {
          const verb = decision === "approve" ? "approved" : "rejected";
          return sendConsentApprovePage(
            res,
            200,
            decision === "approve" ? "Thank you" : "Recorded",
            `<p>${decision === "approve" ? "Your approval is already on file (e.g. double-tap or refresh). Nothing more to do." : "Your decline is already on file (e.g. double-tap or refresh). Nothing more to do."}</p>` +
              `<p>Reference: <strong>${escapeHtml(view.consentId)}</strong><br/>Status: <strong>${escapeHtml(view.status)}</strong></p>` +
              `<p>You can close this page.</p>`
          );
        }
        return res.json({
          ok: true,
          idempotentReplay: true,
          ...view,
        });
      }
      if (html) {
        return sendConsentApprovePage(
          res,
          409,
          "Already completed",
          `<p>This consent was already finalized (status: <strong>${escapeHtml(record.status)}</strong>). ` +
            `You cannot submit a different choice with this link.</p>`
        );
      }
      return res.status(409).json({
        ok: false,
        error: "consent_already_finalized",
        status: record.status,
        attemptedDecision: decision,
      });
    }
    const nowIso = new Date().toISOString();
    if (record.expiresAt && Date.parse(record.expiresAt) <= Date.now()) {
      record.status = "expired";
      record.updatedAt = nowIso;
      consentRecords.set(consentId, record);
      appendConsentEvent(consentId, "expired", {});
      persistStore();
      if (html) {
        return sendConsentApprovePage(
          res,
          410,
          "Link expired",
          `<p>This consent link has expired. Please contact the dealership for a new request.</p>`
        );
      }
      return res.status(410).json({ ok: false, error: "consent_expired" });
    }
    record.status = decision === "approve" ? "approved" : "rejected";
    if (decision === "approve") record.approvedAt = nowIso;
    if (decision === "reject") record.rejectedAt = nowIso;
    record.approvalMeta = {
      ip: req.ip || null,
      userAgent: req.headers["user-agent"] || null,
      approvedFromPublicLink: true,
    };
    record.updatedAt = nowIso;
    consentRecords.set(consentId, record);
    appendConsentEvent(consentId, decision === "approve" ? "approved" : "rejected", {
      ip: req.ip || null,
      userAgent: req.headers["user-agent"] || null,
    });
    if (decision === "approve") {
      // Operational resilience: if duplicate pending consents exist for the same lead/purpose,
      // finalize them as approved too so app polling by a sibling consentId does not remain "pending".
      const targetLeadCorrelationId = String(record.leadCorrelationId || "").trim();
      const targetLeadId = String(record.leadId || "").trim();
      const targetPurpose = String(record.purpose || "").trim().toLowerCase();
      const targetDealerId = String(record.tenantContext?.dealerId || "").trim();
      for (const [otherId, other] of consentRecords.entries()) {
        if (otherId === consentId) continue;
        if (String(other?.status || "").toLowerCase() !== "pending") continue;
        if (String(other?.purpose || "").trim().toLowerCase() !== targetPurpose) continue;
        if (String(other?.tenantContext?.dealerId || "").trim() !== targetDealerId) continue;
        const sameLeadCorrelationId =
          targetLeadCorrelationId &&
          String(other?.leadCorrelationId || "").trim() &&
          String(other?.leadCorrelationId || "").trim() === targetLeadCorrelationId;
        const sameLeadId =
          targetLeadId &&
          String(other?.leadId || "").trim() &&
          String(other?.leadId || "").trim() === targetLeadId;
        if (!sameLeadCorrelationId && !sameLeadId) continue;
        other.status = "approved";
        other.approvedAt = nowIso;
        other.updatedAt = nowIso;
        other.approvalMeta = {
          ip: req.ip || null,
          userAgent: req.headers["user-agent"] || null,
          approvedFromPublicLink: true,
          approvedViaEquivalentConsent: consentId,
        };
        consentRecords.set(otherId, other);
        appendConsentEvent(otherId, "approved_equivalent", {
          sourceConsentId: consentId,
          ip: req.ip || null,
          userAgent: req.headers["user-agent"] || null,
        });
      }
    }
    persistStore();
    if (decision === "approve") {
      // Best-effort follow-up; do not block approval response on email delivery.
      setTimeout(() => {
        void (async () => {
          try {
            const followup = await sendConsentApprovalFollowupEmail(record);
            log("info", "consent_approval_followup_email", {
              consentId,
              emailSent: Boolean(followup?.emailSent),
              providerRef: followup?.providerRef || null,
              warning: followup?.warning || null,
            });
          } catch (e) {
            log("warn", "consent_approval_followup_email_failed", {
              consentId,
              error: e?.message || String(e),
            });
          }
        })();
      }, 0);
    }
    const view = consentPublicView(record);
    if (html) {
      const verb = decision === "approve" ? "approved" : "rejected";
      return sendConsentApprovePage(
        res,
        200,
        verb === "approved" ? "Thank you" : "Recorded",
        decision === "approve"
          ? `<p>Thank you for approving this request.</p><p>You can close this page.</p>`
          : `<p>Your response has been recorded.</p><p>You can close this page.</p>`
      );
    }
    return res.json({
      ok: true,
      ...view,
    });
  } catch (e) {
    const raw = String(e?.message || e || "");
    const status = raw.includes("expired") ? 410 : 400;
    const errCode = raw.includes("expired") ? "token_expired" : "invalid_token";
    if (html) {
      const title = status === 410 ? "Link expired" : "Invalid link";
      return sendConsentApprovePage(
        res,
        status,
        title,
        `<p>${escapeHtml(raw.includes("expired") ? "This link has expired." : "This link is invalid or has expired.")}</p>`
      );
    }
    return res.status(status).json({
      ok: false,
      error: errCode,
      detail: raw,
    });
  }
});

app.post("/api/v1/commands", requireAuth, extractTenantContext, async (req, res) => {
  const normalized = normalizeCommandBody(req.body);
  if (normalized?.error) {
    return sendApiError(req, res, 400, normalized.error);
  }

  let { commandType, correlationId, payload, meta } = normalized;
  if (!SUBMITTABLE_COMMANDS.has(commandType)) {
    return sendApiError(req, res, 400, "unsupported_command_type", `Allowed commandType values: ${Array.from(SUBMITTABLE_COMMANDS).join(", ")}`);
  }
  if (commandType === "CREATE_LEAD" && isPlainObject(payload)) {
    // Normalize source for scan-captured leads; downstream analytics should not classify these as portal imports.
    payload.source = "CubeOneScan";
    payload.leadSource = "id_scan";
  }
  if (commandType === "CREDIT_CHECK" && isPlainObject(payload)) {
    const inferredConsent = inferApprovedConsentIdForCreditCheck(payload, req.tenantContext);
    if (inferredConsent) {
      payload.consentId = inferredConsent;
    }
  }
  const payloadValidation = validateCommandPayload(commandType, payload, req.tenantContext);
  if (payloadValidation) {
    return sendApiError(req, res, 400, payloadValidation.error, payloadValidation.hint || null);
  }
  const role = normalizeRole(req.tenantContext?.role);
  if (!canSubmitCommand(role, commandType)) {
    return sendApiError(req, res, 403, "forbidden", `role ${role} cannot submit ${commandType}`);
  }
  const ownershipCheck = enforceLeadOwnershipForCommand(commandType, payload, req.tenantContext);
  if (ownershipCheck) {
    return sendApiError(req, res, 403, ownershipCheck.error, ownershipCheck.hint);
  }
  if (requiresManagerApproval(role, commandType)) {
    const requestType = `${commandType}_REQUEST`;
    const pendingRecord = {
      correlationId,
      status: "pending_manager_approval",
      commandType: requestType,
      payload,
      meta: { ...meta, requestId: req.requestId },
      tenantContext: req.tenantContext,
      idempotencyKey: null,
      attempts: 0,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      managerSeenAt: null,
      managerSeenByUserId: null,
      result: null,
      error: null,
    };
    createOrUpdateCommand(pendingRecord);
    return res.status(202).json({
      correlationId,
      status: pendingRecord.status,
      commandType: pendingRecord.commandType,
      hint: "Awaiting manager approval",
    });
  }
  const idempotencyKey = String(req.headers["idempotency-key"] || meta?.idempotencyKey || "").trim();

  if (commandType === "CREATE_LEAD" && idempotencyKey && idempotencyIndex.has(idempotencyKey)) {
    const existingCorrelationId = idempotencyIndex.get(idempotencyKey);
    const existing = existingCorrelationId ? commands.get(existingCorrelationId) : null;
    if (existing) {
      return res.status(200).json({
        correlationId: existing.correlationId,
        status: existing.status,
        commandType: existing.commandType,
        deduplicated: true,
      });
    }
  }

  // Fast paths for live UX: process valuation and stock take immediately.
  if (commandType === VALUATION_COMMAND || commandType === "STOCK_TAKE") {
    try {
      const result = await processCommand(commandType, {
        ...payload,
        _tenantContext: req.tenantContext || null,
        _commandCorrelationId: correlationId,
      });
      return res.status(200).json({
        correlationId,
        status: "done",
        commandType,
        result,
      });
    } catch (e) {
      const mapped = mapValuationError(e);
      return res.status(mapped.httpStatus).json({
        correlationId,
        status: "failed",
        commandType,
        error: mapped.error,
        userMessage: mapped.userMessage,
        hint: mapped.hint || null,
        suggestions: mapped.suggestions || null,
      });
    }
  }

  const record = {
    correlationId,
    status: "queued",
    commandType,
    payload,
    meta: { ...meta, requestId: req.requestId },
    tenantContext: req.tenantContext,
    idempotencyKey: commandType === "CREATE_LEAD" && idempotencyKey ? idempotencyKey : null,
    attempts: 0,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    result: null,
    error: null,
  };

  createOrUpdateCommand(record);

  // Simulate async processing.
  (async () => {
    let lastErr = null;
    for (let attempt = 1; attempt <= COMMAND_MAX_RETRIES; attempt++) {
      try {
        record.status = "processing";
        record.attempts = attempt;
        record.updatedAt = new Date().toISOString();
        createOrUpdateCommand(record);

        const result = await processCommand(commandType, {
          ...payload,
          _tenantContext: record.tenantContext || null,
          _commandCorrelationId: record.correlationId,
        });

        record.status = "done";
        record.updatedAt = new Date().toISOString();
        record.result = result;
        if (record.commandType === "CREATE_LEAD" && result?.leadId) {
          assignLeadOwnerIndex(record.correlationId, result.leadId, record.tenantContext || {});
        }
        createOrUpdateCommand(record);
        log("info", "command_completed", {
          correlationId: record.correlationId,
          commandType: record.commandType,
          mode: result?.mode ?? null,
          leadId: result?.leadId ?? null,
          stockUnitId: result?.stockUnitId ?? null,
          warning: result?.warning ?? null,
        });
        return;
      } catch (e) {
        lastErr = e;
        record.error = e?.message || String(e);
        record.updatedAt = new Date().toISOString();
        createOrUpdateCommand(record);
        log("warn", "command_attempt_failed", {
          correlationId: record.correlationId,
          commandType: record.commandType,
          attempt,
          maxRetries: COMMAND_MAX_RETRIES,
          error: record.error,
        });
        if (attempt < COMMAND_MAX_RETRIES) {
          await safeSleep(COMMAND_RETRY_DELAY_MS * attempt);
        }
      }
    }
    record.status = "failed";
    record.updatedAt = new Date().toISOString();
    record.error = (lastErr && (lastErr.message || String(lastErr))) || "Unknown command failure";
    createOrUpdateCommand(record);
    pushDeadLetter(record, record.error);
  })();

  return res.status(202).json({
    correlationId,
    status: record.status,
    commandType,
  });
});

app.get("/api/v1/commands/:correlationId", requireAuth, extractTenantContext, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return sendApiError(req, res, 404, "not_found");
  if (!canAccessTenantScopedRecord(record.tenantContext || {}, req.tenantContext || {})) {
    return sendApiError(req, res, 403, "forbidden", "Command does not belong to current tenant scope.");
  }
  return res.json({
    correlationId: record.correlationId,
    status: record.status,
    commandType: record.commandType,
    managerSeen: Boolean(record.managerSeenAt),
    managerSeenAt: record.managerSeenAt || null,
    managerSeenByUserId: record.managerSeenByUserId || null,
    result: record.result,
    tenantContext: record.tenantContext || null,
    error: record.error,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt
  });
});

app.get("/api/v1/debug/evolvesa/:correlationId", requireAuth, extractTenantContext, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
  if (!canAccessTenantScopedRecord(record.tenantContext || {}, req.tenantContext || {})) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_scope_mismatch" });
  }
  if (record.commandType !== "CREATE_LEAD") {
    return res.status(400).json({ error: "not_create_lead" });
  }
  return res.json({
    correlationId: record.correlationId,
    status: record.status,
    commandType: record.commandType,
    leadId: record?.result?.leadId || null,
    provider: record?.result?.provider || null,
    mode: record?.result?.mode || null,
    tenantContext: record.tenantContext || null,
    evolvesaDebug: record?.result?.evolvesaDebug || null,
    error: record.error || null,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  });
});

app.get("/api/v1/debug/evolvesa-stock/:correlationId", requireAuth, extractTenantContext, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
  if (!canAccessTenantScopedRecord(record.tenantContext || {}, req.tenantContext || {})) {
    return res.status(403).json({ error: "forbidden", hint: "tenant_scope_mismatch" });
  }
  if (record.commandType !== "CREATE_STOCK_UNIT") {
    return res.status(400).json({ error: "not_create_stock_unit" });
  }
  return res.json({
    correlationId: record.correlationId,
    status: record.status,
    commandType: record.commandType,
    stockUnitId: record?.result?.stockUnitId || null,
    provider: record?.result?.provider || null,
    mode: record?.result?.mode || null,
    tenantContext: record.tenantContext || null,
    evolvesaDebug: record?.result?.evolvesaDebug || null,
    error: record.error || null,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  });
});

/**
 * Webhook placeholders (integration direction: systems -> our connector).
 * In v1 these just acknowledge receipt.
 */
app.post("/api/v1/webhooks/:type", requireAuth, (req, res) => {
  const type = req.params.type;
  // TODO: validate webhook signatures per vendor.
  return res.status(200).json({ ok: true, receivedType: type });
});

// Bind all interfaces so phones on the same LAN can reach this PC (not just localhost).
app.listen(PORT, "0.0.0.0", () => {
  loadStore();
  loadAutoTraderCache();
  loadVmgCache();
  log("info", "connector_started", {
    port: PORT,
    bind: "0.0.0.0",
    commandsLoaded: commands.size,
    deadLetters: deadLetters.length,
    storeFile: STORE_FILE,
    tenantRingfence: ENFORCE_TENANT_RINGFENCE,
    mappedUsers: USER_EMAIL_DEALER_MAP.size,
    tenantConfigSource: TENANT_CONFIG.source,
    consentSmtpConfigured: smtpConfigured(),
    consentBrevoApiConfigured: brevoApiConfigured(),
    consentEmailConfigured: consentEmailDispatchConfigured(),
    consentLinkBaseConfigured: Boolean(String(CONSENT_LINK_BASE_URL || "").trim()),
    consentApprovalFollowupEnabled: CONSENT_APPROVAL_FOLLOWUP_EMAIL_ENABLED,
  });
  if (smtpConfigured() && !brevoApiConfigured()) {
    log("warn", "connector_consent_email_smtp_only", {
      note: "Outbound SMTP (25/465/587) is blocked on Render free web services. Set BREVO_API_KEY so consent mail uses Brevo HTTPS API.",
    });
  }
});

