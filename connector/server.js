require("dotenv").config();

const express = require("express");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const path = require("path");
let jwt = null;
try {
  jwt = require("jsonwebtoken");
} catch (_) {
  // Optional dependency; tenant verification will be disabled if missing.
}

const app = express();
app.use(express.json({ limit: "2mb" }));

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
const EVOLVESA_LEAD_RECEIVING_ENTITY_MAP_RAW = (process.env.EVOLVESA_LEAD_RECEIVING_ENTITY_MAP || "").trim();
const EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER_RAW = (process.env.EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER || "").trim();
const EVOLVESA_LEAD_SOURCE = (process.env.EVOLVESA_LEAD_SOURCE || "CubeOneScan").trim();
const EVOLVESA_LEAD_SOURCE_ID = (process.env.EVOLVESA_LEAD_SOURCE_ID || "").trim();
const EVOLVESA_STOCK_SOURCE = (process.env.EVOLVESA_STOCK_SOURCE || "CubeOneScan").trim();
const EVOLVESA_STOCK_SOURCE_ID = (process.env.EVOLVESA_STOCK_SOURCE_ID || "").trim();
const EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA = (process.env.EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA || "Gauteng").trim();
const EVOLVESA_DEFAULT_LEAD_USER_AREA = (process.env.EVOLVESA_DEFAULT_LEAD_USER_AREA || "JHB").trim();
const AUTOTRADER_LISTINGS_URL = (
  process.env.AUTOTRADER_LISTINGS_URL ||
  "https://services.autotrader.co.za/api/syndication/v1.0/listings"
).trim();
const AUTOTRADER_BASIC_AUTH = (process.env.AUTOTRADER_BASIC_AUTH || "").trim();
const AUTOTRADER_TIMEOUT_MS = Number(process.env.AUTOTRADER_TIMEOUT_MS || 12000);
const AUTOTRADER_LISTINGS_CACHE_TTL_MS = Number(process.env.AUTOTRADER_LISTINGS_CACHE_TTL_MS || 300000);
const AUTOTRADER_RATE_LIMIT_COOLDOWN_MS = Number(process.env.AUTOTRADER_RATE_LIMIT_COOLDOWN_MS || 180000);
const VMG_STOCK_FEED_URL = (process.env.VMG_STOCK_FEED_URL || "").trim();
const VMG_DEALER_SCOPES_RAW = (process.env.VMG_DEALER_SCOPES || "509").trim();
const VMG_TIMEOUT_MS = Number(process.env.VMG_TIMEOUT_MS || 12000);
const VMG_CACHE_TTL_MS = Number(process.env.VMG_CACHE_TTL_MS || 300000);
const DEALER_ID_ALIASES_RAW = (process.env.DEALER_ID_ALIASES || "").trim();
const AUTH_JWT_SECRET = (process.env.AUTH_JWT_SECRET || "").trim();
const COMMAND_MAX_RETRIES = Number(process.env.COMMAND_MAX_RETRIES || 3);
const COMMAND_RETRY_DELAY_MS = Number(process.env.COMMAND_RETRY_DELAY_MS || 1200);
const CONNECTOR_DATA_DIR = process.env.CONNECTOR_DATA_DIR || path.join(__dirname, "data");
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

function extractTenantContext(req, _res, next) {
    const userToken = String(req.headers["x-user-token"] || "").trim();
    let claims = {};
    if (userToken && AUTH_JWT_SECRET) {
      try {
        claims = jwt.verify(userToken, AUTH_JWT_SECRET);
      } catch (_) {
        claims = {};
      }
    }
    const headerDealerId = String(req.headers["x-dealer-id"] || "").trim();
    const headerBranchId = String(req.headers["x-branch-id"] || "").trim();
    const headerRole = String(req.headers["x-user-role"] || "").trim();
    req.tenantContext = {
      userId: claims.userId || claims.sub || req.headers["x-user-id"] || null,
      userEmail: claims.email || claims.userEmail || req.headers["x-user-email"] || null,
      userName: claims.name || claims.displayName || claims.fullName || req.headers["x-user-name"] || null,
      // Prefer explicit headers from app settings over token claims for tenant scoping.
      dealerId: headerDealerId || claims.dealerId || null,
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
  "SHARE_LEAD",
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

function normalizeRole(rawRole) {
  const role = String(rawRole || "").trim().toLowerCase();
  if (BUSINESS_ROLES.includes(role)) return role;
  if (["admin", "owner", "superadmin"].includes(role)) return "dealer_principal";
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
      "SHARE_LEAD",
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
    "SHARE_LEAD",
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
const autoTraderListingsCache = {
  fetchedAtMs: 0,
  rows: [],
};
const vmgListingsCache = {
  fetchedAtMs: 0,
  rows: [],
};
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

const VMG_DEALER_SCOPES = parseTokenSet(VMG_DEALER_SCOPES_RAW);

function useVmgFeedForDealer(dealerScope) {
  const dealer = String(dealerScope || "").trim().toLowerCase();
  return Boolean(VMG_STOCK_FEED_URL) && dealer && VMG_DEALER_SCOPES.has(dealer);
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
    const payload = {
      savedAt: new Date().toISOString(),
      fetchedAtMs: vmgListingsCache.fetchedAtMs || 0,
      rows: Array.isArray(vmgListingsCache.rows) ? vmgListingsCache.rows : [],
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
    const rows = Array.isArray(parsed?.rows) ? parsed.rows : [];
    if (rows.length === 0) return;
    vmgListingsCache.rows = rows;
    vmgListingsCache.fetchedAtMs = Number(parsed?.fetchedAtMs || Date.now());
    log("info", "vmg_cache_loaded", {
      rows: rows.length,
      cachedAt: new Date(vmgListingsCache.fetchedAtMs).toISOString(),
    });
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

function validateCommandPayload(commandType, payload) {
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
    case "SHARE_LEAD": {
      if (!payload.leadId && !payload.leadCorrelationId) {
        return { error: "invalid_payload", hint: "SHARE_LEAD requires leadId or leadCorrelationId." };
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
  return {
    firstName: dl.NAMES || dl.firstName || "",
    lastName: dl.SURNAME || dl.lastName || dl.surname || "",
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

function formatEvolvesaLocalDateTime(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
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

const EVOLVESA_LEAD_RECEIVING_ENTITY_MAP = parseKeyValueMap(EVOLVESA_LEAD_RECEIVING_ENTITY_MAP_RAW);
const EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER = parseKeyValueMap(EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER_RAW);

function resolveLeadReceivingEntity(tenantDealerId) {
  const dealerId = String(tenantDealerId || "").trim();
  const mapped = dealerId ? EVOLVESA_LEAD_RECEIVING_ENTITY_MAP.get(dealerId) : null;
  if (mapped) {
    const sep = mapped.indexOf("|");
    if (sep > 0) {
      return {
        id: mapped.slice(0, sep).trim() || dealerId,
        name: mapped.slice(sep + 1).trim() || `Dealer ${dealerId}`,
      };
    }
    return { id: mapped, name: `Dealer ${mapped}` };
  }
  return {
    id: EVOLVESA_LEAD_RECEIVING_ENTITY_ID,
    name: EVOLVESA_LEAD_RECEIVING_ENTITY_NAME,
  };
}

function resolveLeadTriggerUrl(tenantDealerId) {
  const dealerId = String(tenantDealerId || "").trim();
  if (dealerId && EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER.has(dealerId)) {
    return EVOLVESA_LEAD_TRIGGER_URL_BY_DEALER.get(dealerId);
  }
  if (!EVOLVESA_LEAD_TRIGGER_URL) return "";
  try {
    const u = new URL(EVOLVESA_LEAD_TRIGGER_URL);
    if (dealerId && u.searchParams.has("did")) {
      u.searchParams.set("did", dealerId);
      return u.toString();
    }
  } catch (_) {
    // Keep original URL when parsing fails.
  }
  return EVOLVESA_LEAD_TRIGGER_URL;
}

async function evolvesaTriggerCreateLead(payload) {
  const tenant = payload?._tenantContext || {};
  const resolvedTriggerUrl = resolveLeadTriggerUrl(tenant?.dealerId);
  if (!resolvedTriggerUrl) {
    throw new Error("EVOLVESA_LEAD_TRIGGER_URL not configured");
  }

  const dl = payload?.driverLicense || payload?.lead || {};
  const receivingEntity = resolveLeadReceivingEntity(tenant?.dealerId);

  const ancillaryArea = dl?.area || payload?.area || EVOLVESA_DEFAULT_LEAD_ANCILLARY_AREA;
  const userArea = payload?.userArea || dl?.userArea || EVOLVESA_DEFAULT_LEAD_USER_AREA;
  const firstName = dl?.firstName || dl?.NAMES || "";
  const lastName = dl?.lastName || dl?.SURNAME || dl?.surname || "";
  const fullName = `${firstName} ${lastName}`.trim() || dl?.name || payload?.name || "Customer";

  const phone = dl?.phone || dl?.mobile || payload?.phone || "";
  const email = dl?.email || payload?.email || "";
  const idNumber = dl?.idNumber || dl?.ID_NUMBER || payload?.idNumber || "";
  const createdByName = tenant?.userName || "";
  const createdByEmail = tenant?.userEmail || "";
  const createdByUserId = tenant?.userId || "";
  const createdByRole = tenant?.role || "";
  const assignedTo = createdByEmail || createdByName || createdByUserId;

  // “created/lead-reference” + “receiving-entity” are required by the trigger payload structure.
  const leadReference = dl?.idNumber || dl?.LICENSE_NUMBER || dl?.licenseNumber || `lead_${uuidv4()}`;
  const created = formatEvolvesaLocalDateTime(new Date());

  // Minimal “ancillary-data” keeping payload shape, but excluding vehicle detail as requested.
  const requestBody = {
    "ancillary-data": {
      area: ancillaryArea,
      type: "stock",
      source: EVOLVESA_LEAD_SOURCE,
      ...(EVOLVESA_LEAD_SOURCE_ID ? { "source-id": EVOLVESA_LEAD_SOURCE_ID } : {}),
      ...(idNumber ? { "id-number": String(idNumber) } : {}),
      ...(createdByUserId ? { "created-by-user-id": String(createdByUserId) } : {}),
      ...(createdByName ? { "created-by-name": String(createdByName) } : {}),
      ...(createdByEmail ? { "created-by-email": String(createdByEmail) } : {}),
      ...(createdByRole ? { "created-by-role": String(createdByRole) } : {}),
      ...(createdByUserId ? { "assigned-to-user-id": String(createdByUserId) } : {}),
      ...(createdByName ? { "assigned-to-name": String(createdByName) } : {}),
      ...(createdByEmail ? { "assigned-to-email": String(createdByEmail) } : {}),
      ...(assignedTo ? { "assigned-to": String(assignedTo) } : {}),
    },
    created,
    "lead-reference": String(leadReference),
    // Some EvolveSA environments expect lead-source on the lead itself, others in ancillary-data.
    ...(EVOLVESA_LEAD_SOURCE ? { "lead-source": EVOLVESA_LEAD_SOURCE } : {}),
    ...(EVOLVESA_LEAD_SOURCE_ID ? { "lead-source-id": EVOLVESA_LEAD_SOURCE_ID } : {}),
    "receiving-entity": {
      id: receivingEntity.id,
      name: receivingEntity.name,
    },
    "user-data": {
      area: userArea,
      email,
      ...(EVOLVESA_LEAD_SOURCE ? { source: EVOLVESA_LEAD_SOURCE } : {}),
      ...(EVOLVESA_LEAD_SOURCE_ID ? { "source-id": EVOLVESA_LEAD_SOURCE_ID } : {}),
      message:
        dl?.message ||
        payload?.message ||
        `Lead created from ${EVOLVESA_LEAD_SOURCE}. Dealer=${tenant?.dealerId || ""} Branch=${tenant?.branchId || ""}${idNumber ? ` ID=${idNumber}` : ""}`,
      "mobile-number": phone,
      name: fullName,
      ...(createdByUserId ? { "assigned-to-user-id": String(createdByUserId) } : {}),
      ...(createdByName ? { "assigned-to-name": String(createdByName) } : {}),
      ...(createdByEmail ? { "assigned-to-email": String(createdByEmail) } : {}),
      ...(assignedTo ? { "assigned-to": String(assignedTo) } : {}),
      // Duplicate ID number here so Deal Builder / credit tab can bind it directly.
      ...(idNumber ? { "id-number": String(idNumber) } : {}),
    },
  };

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), EVOLVESA_TIMEOUT_MS);
  try {
    const headers = {
      "Content-Type": "application/json",
      Accept: "application/json",
    };
    const authHeaderValue = evolveAuthHeaderValue();
    if (authHeaderValue) {
      headers.Authorization = authHeaderValue;
    }

    const response = await fetch(resolvedTriggerUrl, {
      method: "POST",
      headers,
      body: JSON.stringify(requestBody),
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
      const preview = text && text.length > 700 ? `${text.slice(0, 700)}…` : text;
      throw new Error(`EvolveSA lead trigger HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
    }

    // Trigger responses vary; try several shapes.
    let leadId = null;
    if (typeof json === "number") leadId = String(json);
    if (Array.isArray(json) && json.length > 0) {
      const first = json[0];
      leadId =
        (typeof first === "number" && String(first)) ||
        first?.leadId ||
        first?.id ||
        null;
    }
    if (!leadId && json && typeof json === "object") {
      leadId = json.leadId || json.id || json.reference || null;
    }

    const resolvedLeadId = leadId || `lead_${uuidv4()}`;
    log("info", "evolvesa_lead_trigger_ok", {
      httpStatus: response.status,
      leadId: resolvedLeadId,
      responseType: json == null ? "empty" : Array.isArray(json) ? "array" : typeof json,
    });

    return {
      mode: "live",
      provider: "EvolveSA",
      leadId: resolvedLeadId,
      rawRef: json?.reference || json?.ref || null,
      debug: {
        mode: "trigger",
        request: {
          url: redactEvolveTriggerUrl(resolvedTriggerUrl),
          body: requestBody,
        },
        response: {
          status: response.status,
          bodyText: text,
          bodyJson: json,
        },
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function evolvesaCreateLead(payload) {
  if (!EVOLVESA_LEAD_TRIGGER_URL && !evolveConfigured()) {
    throw new Error(
      "EvolveSA lead integration not configured. Set EVOLVESA_BASE_URL and EVOLVESA_API_KEY (or EVOLVESA_LEAD_TRIGGER_URL) in connector .env."
    );
  }

  // Prefer the trigger URL mapping if configured (matches the Cars.co.za → EvolveSA flow you shared).
  if (EVOLVESA_LEAD_TRIGGER_URL) {
    try {
      return await evolvesaTriggerCreateLead(payload);
    } catch (e) {
      throw e;
    }
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), EVOLVESA_TIMEOUT_MS);
  try {
    const body = toEvolveLeadPayload(payload);
    const response = await fetch(`${EVOLVESA_BASE_URL}/api/v1/leads`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": evolveAuthHeaderValue(),
      },
      body: JSON.stringify(body),
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
      const preview = text && text.length > 600 ? `${text.slice(0, 600)}…` : text;
      throw new Error(`EvolveSA HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
    }

    return {
      mode: "live",
      leadId: json?.leadId || json?.id || `lead_${uuidv4()}`,
      provider: "EvolveSA",
      rawRef: json?.reference || json?.ref || null,
      debug: {
        mode: "api",
        request: {
          url: `${EVOLVESA_BASE_URL}/api/v1/leads`,
          body,
        },
        response: {
          status: response.status,
          bodyText: text,
          bodyJson: json,
        },
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function evolvesaCreateStockUnit(payload) {
  if (EVOLVESA_STOCK_TRIGGER_URL) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), EVOLVESA_TIMEOUT_MS);
    try {
      const body = toEvolveStockPayload(payload);
      const headers = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };
      const authHeaderValue = evolveAuthHeaderValue();
      if (authHeaderValue) {
        headers.Authorization = authHeaderValue;
      }
      const response = await fetch(EVOLVESA_STOCK_TRIGGER_URL, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
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
        const preview = text && text.length > 600 ? `${text.slice(0, 600)}…` : text;
        throw new Error(`EvolveSA stock trigger HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
      }
      return {
        mode: "live",
        provider: "EvolveSA",
        stockUnitId: json?.stockUnitId || json?.id || `stock_${uuidv4()}`,
        rawRef: json?.reference || json?.ref || null,
        debug: {
          mode: "trigger",
          request: {
            url: redactEvolveTriggerUrl(EVOLVESA_STOCK_TRIGGER_URL),
            body,
          },
          response: {
            status: response.status,
            bodyText: text,
            bodyJson: json,
          },
        },
      };
    } finally {
      clearTimeout(timeout);
    }
  }

  if (!evolveConfigured()) {
    return {
      mode: "stub",
      stockUnitId: `stock_${uuidv4()}`,
      warning:
        "EvolveSA stock integration not configured. Set EVOLVESA_STOCK_TRIGGER_URL or EVOLVESA_BASE_URL + EVOLVESA_API_KEY in connector .env.",
      stock: toEvolveStockPayload(payload),
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), EVOLVESA_TIMEOUT_MS);
  try {
    const body = toEvolveStockPayload(payload);
    const endpoint = EVOLVESA_STOCK_ENDPOINT.startsWith("/") ? EVOLVESA_STOCK_ENDPOINT : `/${EVOLVESA_STOCK_ENDPOINT}`;
    const response = await fetch(`${EVOLVESA_BASE_URL}${endpoint}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": evolveAuthHeaderValue(),
      },
      body: JSON.stringify(body),
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
      const preview = text && text.length > 600 ? `${text.slice(0, 600)}…` : text;
      throw new Error(`EvolveSA stock HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
    }
    return {
      mode: "live",
      provider: "EvolveSA",
      stockUnitId: json?.stockUnitId || json?.id || `stock_${uuidv4()}`,
      rawRef: json?.reference || json?.ref || null,
      debug: {
        mode: "api",
        request: {
          url: `${EVOLVESA_BASE_URL}${endpoint}`,
          body,
        },
        response: {
          status: response.status,
          bodyText: text,
          bodyJson: json,
        },
      },
    };
  } finally {
    clearTimeout(timeout);
  }
}

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
  return String(value || "").trim().toLowerCase();
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

const DEALER_ID_ALIASES = parseDealerAliases(DEALER_ID_ALIASES_RAW);

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

async function fetchVmgListingsCached(forceRefresh = false) {
  if (!VMG_STOCK_FEED_URL) {
    throw new Error("VMG stock feed is not configured");
  }
  const now = Date.now();
  if (!forceRefresh && vmgListingsCache.rows.length > 0 && now - vmgListingsCache.fetchedAtMs < VMG_CACHE_TTL_MS) {
    return {
      rows: vmgListingsCache.rows,
      source: "vmg_cache",
      cachedAt: new Date(vmgListingsCache.fetchedAtMs).toISOString(),
    };
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), VMG_TIMEOUT_MS);
  try {
    const response = await fetch(VMG_STOCK_FEED_URL, {
      method: "GET",
      headers: { Accept: "application/xml,text/xml,*/*" },
      signal: controller.signal,
    });
    const text = await response.text();
    if (!response.ok) {
      throw new Error(`VMG stock feed HTTP ${response.status}${text ? `: ${text.slice(0, 500)}` : ""}`);
    }
    const normalized = parseVmgStockXml(text);
    vmgListingsCache.rows = normalized;
    vmgListingsCache.fetchedAtMs = Date.now();
    persistVmgCache();
    return {
      rows: normalized,
      source: "vmg_upstream",
      cachedAt: new Date(vmgListingsCache.fetchedAtMs).toISOString(),
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
      const evolve = await evolvesaCreateLead(payload);
      result.leadId = evolve.leadId;
      result.provider = "EvolveSA";
      result.mode = evolve.mode;
      if (evolve.warning) result.warning = evolve.warning;
      if (evolve.rawRef) result.rawRef = evolve.rawRef;
      if (evolve.debug) result.evolvesaDebug = evolve.debug;
      result.lead = payload?.driverLicense || payload?.lead || {};
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
    case "CREATE_STOCK_UNIT": {
      const evolve = await evolvesaCreateStockUnit(payload);
      result.stockUnitId = evolve.stockUnitId;
      result.provider = "EvolveSA";
      result.mode = evolve.mode;
      if (evolve.warning) result.warning = evolve.warning;
      if (evolve.rawRef) result.rawRef = evolve.rawRef;
      if (evolve.debug) result.evolvesaDebug = evolve.debug;
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

      let fetched;
      try {
        fetched = await fetchAutoTraderListingsCached(false);
      } catch (e) {
        fetched = {
          rows: Array.isArray(autoTraderListingsCache.rows) ? autoTraderListingsCache.rows : [],
          source: "cache_only_on_error",
          cachedAt: autoTraderListingsCache.fetchedAtMs ? new Date(autoTraderListingsCache.fetchedAtMs).toISOString() : null,
        };
        result.stockLookupWarning = e?.message || String(e);
      }

      const dealerRows = fetched.rows.filter((row) => stockMatchesDealerScope(row, dealerScope));
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
        result.warning = "No AutoTrader stock match found for the scanned barcode.";
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
        ? await fetchVmgListingsCached(forceRefresh)
        : await fetchAutoTraderListingsCached(forceRefresh);
    } catch (refreshErr) {
      if (!forceRefresh) throw refreshErr;
      // Upstream can intermittently fail on forced refresh.
      // Serve last known cache instead of hard-failing stock lookup in the app.
      fetched = useVmgFeed
        ? await fetchVmgListingsCached(false)
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
    const providerCacheRows = useVmgFeed ? vmgListingsCache.rows : autoTraderListingsCache.rows;
    const providerFetchedAtMs = useVmgFeed ? vmgListingsCache.fetchedAtMs : autoTraderListingsCache.fetchedAtMs;
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

app.post("/api/v1/commands", requireAuth, extractTenantContext, async (req, res) => {
  const normalized = normalizeCommandBody(req.body);
  if (normalized?.error) {
    return sendApiError(req, res, 400, normalized.error);
  }

  const { commandType, correlationId, payload, meta } = normalized;
  if (!SUBMITTABLE_COMMANDS.has(commandType)) {
    return sendApiError(req, res, 400, "unsupported_command_type", `Allowed commandType values: ${Array.from(SUBMITTABLE_COMMANDS).join(", ")}`);
  }
  const payloadValidation = validateCommandPayload(commandType, payload);
  if (payloadValidation) {
    return sendApiError(req, res, 400, payloadValidation.error, payloadValidation.hint || null);
  }
  const role = normalizeRole(req.tenantContext?.role);
  if (!canSubmitCommand(role, commandType)) {
    return sendApiError(req, res, 403, "forbidden", `role ${role} cannot submit ${commandType}`);
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
      const result = await processCommand(commandType, { ...payload, _tenantContext: req.tenantContext || null });
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

        const result = await processCommand(commandType, { ...payload, _tenantContext: record.tenantContext || null });

        record.status = "done";
        record.updatedAt = new Date().toISOString();
        record.result = result;
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

app.get("/api/v1/commands/:correlationId", requireAuth, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return sendApiError(req, res, 404, "not_found");
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

app.get("/api/v1/debug/evolvesa/:correlationId", requireAuth, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
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

app.get("/api/v1/debug/evolvesa-stock/:correlationId", requireAuth, (req, res) => {
  const correlationId = req.params.correlationId;
  const record = commands.get(correlationId);
  if (!record) return res.status(404).json({ error: "not_found" });
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
  });
});

