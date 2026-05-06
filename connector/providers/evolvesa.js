function formatEvolvesaLocalDateTime(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
}

function normalizeWhitespace(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Node's undici fetch often throws only "fetch failed"; err.cause has ENOTFOUND, ECONNREFUSED, etc.
 */
function enrichEvolvesaFetchError(err, operationLabel) {
  if (!err) return new Error(`EvolveSA ${operationLabel}: unknown error`);
  const msg = String(err.message || err);
  if (err.name === "AbortError") {
    return new Error(
      `EvolveSA ${operationLabel} timed out (no response in time). Check CRM availability or connector timeout.`,
      { cause: err }
    );
  }
  const cause = err.cause;
  const code = cause && cause.code != null ? String(cause.code) : "";
  const causeMsg = cause && (cause.message || String(cause)) ? String(cause.message || cause) : "";
  const isFetchFailed = msg === "fetch failed" || /fetch failed/i.test(msg);
  const networkish =
    isFetchFailed ||
    code === "ENOTFOUND" ||
    code === "ECONNREFUSED" ||
    code === "ECONNRESET" ||
    code === "ETIMEDOUT" ||
    code === "CERT_HAS_EXPIRED" ||
    (causeMsg && /cert|ssl|tls|handshake|UNABLE_TO_VERIFY/i.test(causeMsg));

  if (!networkish) return err;

  const detail = [code, causeMsg].filter(Boolean).join(" — ") || msg;
  const hint =
    code === "ENOTFOUND"
      ? "DNS did not resolve — verify EVOLVESA_BASE_URL or EVOLVESA_LEAD_TRIGGER_URL hostname from the connector host."
      : code === "ECONNREFUSED"
        ? "Connection refused — wrong host/port, CRM down, or firewall blocking the connector server."
        : code === "ECONNRESET" || code === "ETIMEDOUT"
          ? "Connection dropped or timed out — unstable network, VPN, or firewall between connector and CRM."
          : causeMsg && /cert|ssl|tls|handshake|UNABLE_TO_VERIFY/i.test(causeMsg)
            ? "TLS/certificate problem — trust store, expired cert, or HTTPS proxy."
            : "Connector could not complete HTTPS to EvolveSA (path from server to CRM).";

  return new Error(`EvolveSA ${operationLabel} failed (${detail}). ${hint}`, { cause: err });
}

function resolveLeadNames(payload) {
  const dl = payload?.driverLicense || payload?.lead || {};
  const combinedCandidate = normalizeWhitespace(dl?.name || payload?.name || "");
  let firstName = normalizeWhitespace(dl?.firstName || dl?.NAMES || payload?.firstName || "");
  let lastName = normalizeWhitespace(dl?.lastName || dl?.SURNAME || dl?.surname || payload?.lastName || payload?.surname || "");

  if ((!firstName || !lastName) && combinedCandidate) {
    const parts = combinedCandidate.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      if (!lastName) lastName = parts[parts.length - 1];
      if (!firstName) firstName = parts.slice(0, -1).join(" ");
    } else if (!firstName) {
      firstName = parts[0] || "";
    }
  }

  if (firstName && lastName && firstName.toLowerCase() === lastName.toLowerCase() && combinedCandidate) {
    const parts = combinedCandidate.split(" ").filter(Boolean);
    if (parts.length >= 2) {
      firstName = parts.slice(0, -1).join(" ");
      lastName = parts[parts.length - 1];
    }
  }

  const fullName = normalizeWhitespace(`${firstName} ${lastName}`) || combinedCandidate || "Customer";
  return { firstName, lastName, fullName };
}

function createEvolvesaProvider(deps) {
  const {
    fetchImpl,
    uuidv4,
    log,
    canonicalDealerId,
    evolveAuthHeaderValue,
    evolveConfigured,
    toEvolveLeadPayload,
    toEvolveStockPayload,
    redactEvolveTriggerUrl,
    config,
    maps,
  } = deps;

  const {
    baseUrl,
    timeoutMs,
    stockEndpoint,
    stockTriggerUrl,
    leadTriggerUrl,
    leadReceivingEntityId,
    leadReceivingEntityName,
    leadSource,
    leadSourceId,
    defaultLeadAncillaryArea,
    defaultLeadUserArea,
    communicationEndpoint,
  } = config;

  async function evolveFetch(operationLabel, fn) {
    try {
      return await fn();
    } catch (err) {
      const wrapped = enrichEvolvesaFetchError(err, operationLabel);
      throw wrapped;
    }
  }

  function resolveLeadReceivingEntity(tenantDealerId) {
    const dealerId = canonicalDealerId(tenantDealerId);
    const mapped = dealerId ? maps.leadReceivingEntityMap.get(dealerId) : null;
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
      id: leadReceivingEntityId,
      name: leadReceivingEntityName,
    };
  }

  function resolveLeadTriggerUrl(tenantDealerId) {
    const dealerId = canonicalDealerId(tenantDealerId);
    if (dealerId && maps.leadTriggerUrlByDealer.has(dealerId)) {
      const mappedUrl = String(maps.leadTriggerUrlByDealer.get(dealerId) || "").trim();
      // Guardrail: ignore placeholder configs and fallback to global trigger URL with did substitution.
      if (mappedUrl && !mappedUrl.includes("<trigger-id>")) {
        return mappedUrl;
      }
    }
    if (!leadTriggerUrl) return "";
    try {
      const u = new URL(leadTriggerUrl);
      if (dealerId && u.searchParams.has("did")) {
        u.searchParams.set("did", dealerId);
        return u.toString();
      }
    } catch (_) {
      // Keep original URL when parsing fails.
    }
    return leadTriggerUrl;
  }

  async function triggerCreateLead(payload) {
    const tenant = payload?._tenantContext || {};
    const resolvedTriggerUrl = resolveLeadTriggerUrl(tenant?.dealerId);
    if (!resolvedTriggerUrl) {
      throw new Error("EVOLVESA_LEAD_TRIGGER_URL not configured");
    }

    const dl = payload?.driverLicense || payload?.lead || {};
    const receivingEntity = resolveLeadReceivingEntity(tenant?.dealerId);

    const ancillaryArea = dl?.area || payload?.area || defaultLeadAncillaryArea;
    const userArea = payload?.userArea || dl?.userArea || defaultLeadUserArea;
    const { firstName, lastName, fullName } = resolveLeadNames(payload);

    const phone = dl?.phone || dl?.mobile || payload?.phone || "";
    const email = dl?.email || payload?.email || "";
    const idNumber = dl?.idNumber || dl?.ID_NUMBER || payload?.idNumber || "";
    const createdByName = tenant?.userName || "";
    const createdByEmail = tenant?.userEmail || "";
    const createdByUserId = tenant?.userId || "";
    const createdByRole = tenant?.role || "";
    const assignedTo = createdByEmail || createdByName || createdByUserId;
    const resolvedLeadSource =
      String(payload?.leadSource || payload?.source || leadSource || "").trim() || "CubeOneScan";
    const resolvedLeadSourceId = String(payload?.leadSourceId || payload?.sourceId || leadSourceId || "").trim();
    const leadReference = dl?.idNumber || dl?.LICENSE_NUMBER || dl?.licenseNumber || `lead_${uuidv4()}`;
    const created = formatEvolvesaLocalDateTime(new Date());

    const requestBody = {
      "ancillary-data": {
        area: ancillaryArea,
        type: "stock",
        source: resolvedLeadSource,
        ...(resolvedLeadSourceId ? { "source-id": resolvedLeadSourceId } : {}),
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
      ...(resolvedLeadSource ? { "lead-source": resolvedLeadSource } : {}),
      ...(resolvedLeadSourceId ? { "lead-source-id": resolvedLeadSourceId } : {}),
      ...(assignedTo ? { "assigned-to": String(assignedTo) } : {}),
      ...(createdByEmail ? { "assigned-to-email": String(createdByEmail) } : {}),
      ...(createdByName ? { "assigned-to-name": String(createdByName) } : {}),
      ...(createdByUserId ? { "assigned-to-user-id": String(createdByUserId) } : {}),
      "receiving-entity": {
        id: receivingEntity.id,
        name: receivingEntity.name,
      },
      "user-data": {
        area: userArea,
        email,
        ...(resolvedLeadSource ? { source: resolvedLeadSource } : {}),
        ...(resolvedLeadSourceId ? { "source-id": resolvedLeadSourceId } : {}),
        message:
          dl?.message ||
          payload?.message ||
          `Lead created from ${resolvedLeadSource}. Dealer=${tenant?.dealerId || ""} Branch=${tenant?.branchId || ""}${idNumber ? ` ID=${idNumber}` : ""}`,
        "mobile-number": phone,
        name: firstName || fullName,
        ...(lastName ? { surname: lastName } : {}),
        ...(fullName ? { "full-name": fullName } : {}),
        ...(createdByUserId ? { "assigned-to-user-id": String(createdByUserId) } : {}),
        ...(createdByName ? { "assigned-to-name": String(createdByName) } : {}),
        ...(createdByEmail ? { "assigned-to-email": String(createdByEmail) } : {}),
        ...(assignedTo ? { "assigned-to": String(assignedTo) } : {}),
        ...(idNumber ? { "id-number": String(idNumber) } : {}),
      },
    };

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const headers = {
        "Content-Type": "application/json",
        Accept: "application/json",
      };
      const authHeaderValue = evolveAuthHeaderValue();
      if (authHeaderValue) headers.Authorization = authHeaderValue;

      const response = await evolveFetch("create lead (trigger POST)", () =>
        fetchImpl(resolvedTriggerUrl, {
          method: "POST",
          headers,
          body: JSON.stringify(requestBody),
          signal: controller.signal,
        })
      );

      const text = await response.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch (_) {
        json = null;
      }

      if (!response.ok) {
        const preview = text && text.length > 700 ? `${text.slice(0, 700)}...` : text;
        throw new Error(`EvolveSA lead trigger HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
      }

      const payloadStatus = Number(json?.status);
      const payloadCode = String(json?.code || "").trim();
      const payloadName = String(json?.name || "").trim();
      const hasPayloadError =
        (Number.isFinite(payloadStatus) && payloadStatus >= 400) ||
        payloadCode.toUpperCase() === "INVALID_FOREIGN_KEY" ||
        payloadName.toLowerCase().includes("error");
      if (hasPayloadError) {
        const preview = text && text.length > 700 ? `${text.slice(0, 700)}...` : text;
        throw new Error(`EvolveSA lead trigger payload error${preview ? `: ${preview}` : ""}`);
      }

      let leadId = null;
      if (typeof json === "number") leadId = String(json);
      if (Array.isArray(json) && json.length > 0) {
        const first = json[0];
        leadId = (typeof first === "number" && String(first)) || first?.leadId || first?.id || null;
      }
      if (!leadId && json && typeof json === "object") {
        leadId = json.leadId || json.id || json.reference || null;
      }

      const resolvedLeadId = leadId || `lead_${uuidv4()}`;
      const responsePreview = text ? (text.length > 2000 ? `${text.slice(0, 2000)}...` : text) : "";

      log("info", "evolvesa_lead_trigger_ok", {
        httpStatus: response.status,
        leadId: resolvedLeadId,
        dealerId: canonicalDealerId(tenant?.dealerId),
        triggerUrl: redactEvolveTriggerUrl(resolvedTriggerUrl),
        responseType: json == null ? "empty" : Array.isArray(json) ? "array" : typeof json,
        rawResponsePreview: responsePreview,
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

  async function createLead(payload) {
    if (!leadTriggerUrl && !evolveConfigured()) {
      throw new Error(
        "EvolveSA lead integration not configured. Set EVOLVESA_BASE_URL and EVOLVESA_API_KEY (or EVOLVESA_LEAD_TRIGGER_URL) in connector .env."
      );
    }

    if (leadTriggerUrl) {
      return triggerCreateLead(payload);
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const body = toEvolveLeadPayload(payload);
      const response = await evolveFetch("create lead (API POST)", () =>
        fetchImpl(`${baseUrl}/api/v1/leads`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: evolveAuthHeaderValue(),
          },
          body: JSON.stringify(body),
          signal: controller.signal,
        })
      );
      const text = await response.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch (_) {
        json = null;
      }
      if (!response.ok) {
        const preview = text && text.length > 600 ? `${text.slice(0, 600)}...` : text;
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
            url: `${baseUrl}/api/v1/leads`,
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

  async function createStockUnit(payload) {
    if (stockTriggerUrl) {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), timeoutMs);
      try {
        const body = toEvolveStockPayload(payload);
        const headers = {
          "Content-Type": "application/json",
          Accept: "application/json",
        };
        const authHeaderValue = evolveAuthHeaderValue();
        if (authHeaderValue) headers.Authorization = authHeaderValue;
        const response = await evolveFetch("create stock (trigger POST)", () =>
          fetchImpl(stockTriggerUrl, {
            method: "POST",
            headers,
            body: JSON.stringify(body),
            signal: controller.signal,
          })
        );
        const text = await response.text();
        let json = null;
        try {
          json = text ? JSON.parse(text) : null;
        } catch (_) {
          json = null;
        }
        if (!response.ok) {
          const preview = text && text.length > 600 ? `${text.slice(0, 600)}...` : text;
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
              url: redactEvolveTriggerUrl(stockTriggerUrl),
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
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const body = toEvolveStockPayload(payload);
      const endpoint = stockEndpoint.startsWith("/") ? stockEndpoint : `/${stockEndpoint}`;
      const response = await evolveFetch("create stock (API POST)", () =>
        fetchImpl(`${baseUrl}${endpoint}`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
            Authorization: evolveAuthHeaderValue(),
          },
          body: JSON.stringify(body),
          signal: controller.signal,
        })
      );
      const text = await response.text();
      let json = null;
      try {
        json = text ? JSON.parse(text) : null;
      } catch (_) {
        json = null;
      }
      if (!response.ok) {
        const preview = text && text.length > 600 ? `${text.slice(0, 600)}...` : text;
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
            url: `${baseUrl}${endpoint}`,
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

  function resolveCommunicationUrl(leadId, pathValue) {
    const custom = String(communicationEndpoint || "").trim();
    if (custom) {
      try {
        return new URL(custom, baseUrl).toString();
      } catch (_) {
        return custom;
      }
    }
    const p = String(pathValue || `/leads/leads/${leadId}`).trim();
    try {
      return new URL(p, baseUrl).toString();
    } catch (_) {
      return `${baseUrl}${p.startsWith("/") ? p : `/${p}`}`;
    }
  }

  async function logCommunication(payload) {
    const tenant = payload?._tenantContext || {};
    const leadId = String(payload?.leadId || "").trim();
    if (!leadId) {
      throw new Error("leadId is required for communication logging");
    }
    const content = String(payload?.comment?.content || payload?.content || "").trim();
    if (!content) {
      throw new Error("communication content is required");
    }

    const nowIso = String(payload?._serverNowIso || new Date().toISOString());
    const commentId = String(payload?.comment?.id || `c_${uuidv4()}`);
    const fullName = String(
      payload?.comment?.fullname ||
      tenant?.userName ||
      "You"
    ).trim() || "You";
    const profilePictureUrl = String(payload?.comment?.profile_picture_url || "").trim();
    const entityClassRaw = String(payload?.entityClass || "Modules\\Leads\\Entities\\Lead").replace(/^"+|"+$/g, "");
    const entityClassQuoted = `"${entityClassRaw}"`;
    const entityClassHtmlQuoted = `&quot;${entityClassRaw}&quot;`;
    const pathValue = String(payload?.path || `/leads/leads/${leadId}`);
    const postUrl = resolveCommunicationUrl(leadId, pathValue);

    function buildForm(entityClassValue) {
      const form = new URLSearchParams();
      form.append("comment[id]", commentId);
      form.append("comment[parent]", String(payload?.comment?.parent || ""));
      form.append("comment[created]", String(payload?.comment?.created || nowIso));
      form.append("comment[modified]", String(payload?.comment?.modified || nowIso));
      form.append("comment[content]", content);
      form.append("comment[fullname]", fullName);
      form.append("comment[profile_picture_url]", profilePictureUrl);
      form.append("comment[created_by_current_user]", String(payload?.comment?.created_by_current_user ?? true));
      form.append("comment[upvote_count]", String(payload?.comment?.upvote_count ?? 0));
      form.append("comment[user_has_upvoted]", String(payload?.comment?.user_has_upvoted ?? false));
      form.append("entityId", leadId);
      form.append("entityClass", entityClassValue);
      form.append("path", pathValue);
      return form.toString();
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const headers = {
        Accept: "application/json, text/plain, */*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
      };
      const authHeaderValue = evolveAuthHeaderValue();
      if (authHeaderValue) headers.Authorization = authHeaderValue;

      let response = await evolveFetch("log communication (POST)", () =>
        fetchImpl(postUrl, {
          method: "POST",
          headers,
          body: buildForm(entityClassQuoted),
          signal: controller.signal,
        })
      );
      let text = await response.text();
      if (!response.ok && response.status === 400) {
        response = await evolveFetch("log communication (POST, entityClass retry)", () =>
          fetchImpl(postUrl, {
            method: "POST",
            headers,
            body: buildForm(entityClassRaw),
            signal: controller.signal,
          })
        );
        text = await response.text();
      }
      if (!response.ok && response.status === 400) {
        response = await evolveFetch("log communication (POST, HTML entity retry)", () =>
          fetchImpl(postUrl, {
            method: "POST",
            headers,
            body: buildForm(entityClassHtmlQuoted),
            signal: controller.signal,
          })
        );
        text = await response.text();
      }
      if (!response.ok) {
        const preview = text && text.length > 700 ? `${text.slice(0, 700)}...` : text;
        throw new Error(`EvolveSA communication HTTP ${response.status}${preview ? `: ${preview}` : ""}`);
      }

      return {
        id: commentId,
        entityId: leadId,
        channel: String(payload?.channel || "note"),
        status: "logged",
        content,
        postedAt: nowIso,
        path: pathValue,
        endpoint: postUrl,
        responsePreview: text ? (text.length > 400 ? `${text.slice(0, 400)}...` : text) : "",
      };
    } finally {
      clearTimeout(timeout);
    }
  }

  return {
    providerName: "EvolveSA",
    createLead,
    createStockUnit,
    logCommunication,
  };
}

module.exports = {
  createEvolvesaProvider,
};
