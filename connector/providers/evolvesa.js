function formatEvolvesaLocalDateTime(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  const HH = String(d.getHours()).padStart(2, "0");
  const MM = String(d.getMinutes()).padStart(2, "0");
  const SS = String(d.getSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${HH}:${MM}:${SS}`;
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
  } = config;

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
      return maps.leadTriggerUrlByDealer.get(dealerId);
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
    const leadReference = dl?.idNumber || dl?.LICENSE_NUMBER || dl?.licenseNumber || `lead_${uuidv4()}`;
    const created = formatEvolvesaLocalDateTime(new Date());

    const requestBody = {
      "ancillary-data": {
        area: ancillaryArea,
        type: "stock",
        source: leadSource,
        ...(leadSourceId ? { "source-id": leadSourceId } : {}),
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
      ...(leadSource ? { "lead-source": leadSource } : {}),
      ...(leadSourceId ? { "lead-source-id": leadSourceId } : {}),
      "receiving-entity": {
        id: receivingEntity.id,
        name: receivingEntity.name,
      },
      "user-data": {
        area: userArea,
        email,
        ...(leadSource ? { source: leadSource } : {}),
        ...(leadSourceId ? { "source-id": leadSourceId } : {}),
        message:
          dl?.message ||
          payload?.message ||
          `Lead created from ${leadSource}. Dealer=${tenant?.dealerId || ""} Branch=${tenant?.branchId || ""}${idNumber ? ` ID=${idNumber}` : ""}`,
        "mobile-number": phone,
        name: fullName,
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

      const response = await fetchImpl(resolvedTriggerUrl, {
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
      const response = await fetchImpl(`${baseUrl}/api/v1/leads`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Authorization: evolveAuthHeaderValue(),
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
        const response = await fetchImpl(stockTriggerUrl, {
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
      const response = await fetchImpl(`${baseUrl}${endpoint}`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          Authorization: evolveAuthHeaderValue(),
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

  return {
    providerName: "EvolveSA",
    createLead,
    createStockUnit,
  };
}

module.exports = {
  createEvolvesaProvider,
};
