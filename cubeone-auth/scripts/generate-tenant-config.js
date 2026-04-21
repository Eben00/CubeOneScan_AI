require("dotenv").config();

const fs = require("fs");
const path = require("path");

function parseMap(raw) {
  const out = {};
  const text = String(raw || "").trim();
  if (!text) return out;
  for (const token of text.split(",")) {
    const pair = token.trim();
    if (!pair) continue;
    const idx = pair.indexOf("=");
    if (idx <= 0 || idx >= pair.length - 1) continue;
    const k = pair.slice(0, idx).trim();
    const v = pair.slice(idx + 1).trim();
    if (k && v) out[k] = v;
  }
  return out;
}

const outputPath = process.argv[2]
  ? path.resolve(process.argv[2])
  : path.resolve(__dirname, "..", "tenant-config.generated.json");

const payload = {
  dealerAliases: parseMap(process.env.AUTH_DEALER_ID_ALIASES),
  emailDealerMap: parseMap(process.env.AUTH_EMAIL_DEALER_MAP),
};

fs.writeFileSync(outputPath, JSON.stringify(payload, null, 2), "utf8");
console.log(JSON.stringify({ ok: true, outputPath }, null, 2));
