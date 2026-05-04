package com.cubeone.scan.services

import android.content.Context
import android.content.SharedPreferences
import android.util.Base64
import android.util.Log
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuthStore
import org.json.JSONArray
import org.json.JSONObject
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.SocketTimeoutException
import java.net.HttpURLConnection
import java.net.URL

object CommandApiService {
    private const val TAG = "CommandApiService"
    /** Connector sends email synchronously on POST /consents; SMTP (e.g. Brevo) often exceeds 20s. */
    private const val READ_TIMEOUT_MS_CONSENT = 120_000
    private const val QUEUE_PREFS = "command_api_queue"
    private const val QUEUE_KEY = "queued_commands"
    private const val HEALTH_PREFS = "command_api_health"
    private const val HEALTH_LAST_ERROR = "last_error"
    private const val HEALTH_LAST_UPDATE_MS = "last_update_ms"
    private const val HEALTH_LAST_FLUSH_SENT = "last_flush_sent"
    private const val HEALTH_LAST_FLUSH_REMAINING = "last_flush_remaining"

    data class CreateCommandResponse(
        val correlationId: String,
        val status: String,
        val commandType: String,
        val result: JSONObject? = null
    )

    data class CommandStatusResponse(
        val correlationId: String,
        val status: String,
        val commandType: String,
        val managerSeen: Boolean = false,
        val managerSeenAt: String? = null,
        val result: JSONObject?,
        val error: String?
    )

    data class ApprovalItem(
        val correlationId: String,
        val status: String,
        val commandType: String,
        val requestedCommandType: String?,
        val createdAt: String,
        val managerSeen: Boolean,
        val managerSeenAt: String?,
        val payload: JSONObject?
    )

    data class StockItem(
        val stockNumber: String,
        val registrationNumber: String,
        val make: String,
        val model: String,
        val variant: String,
        val year: String,
        val price: String,
        val autoTraderPrice: String,
        val tradePrice: String,
        val retailPrice: String,
        val marketPrice: String,
        val valuationStatus: String,
        val primaryImageUrl: String?,
        val raw: JSONObject?
    ) {
        fun label(): String {
            val vehicle = listOf(make, model, variant).filter { it.isNotBlank() }.joinToString(" ")
            val yearPart = if (year.isBlank()) "" else " ($year)"
            val regPart = if (registrationNumber.isBlank()) "" else " • $registrationNumber"
            val stockPart = if (stockNumber.isBlank()) "" else " • $stockNumber"
            return "${vehicle.ifBlank { "Stock item" }}$yearPart$regPart$stockPart"
        }
    }

    data class StockListResponse(
        val stocks: List<StockItem>,
        val cachedAt: String?,
        val source: String?,
        val total: Int,
        val provider: String?,
        val dealerScope: String?,
        val warning: String?
    )

    data class JsonEnvelopeResponse(
        val ok: Boolean,
        val json: JSONObject
    )

    data class ConsentStatusResponse(
        val consentId: String,
        val status: String,
        val purpose: String?,
        val noticeVersion: String?,
        val requestedAt: String?,
        val expiresAt: String?,
        val approvedAt: String?,
        val rejectedAt: String?,
        val revokedAt: String?,
        val leadCorrelationId: String?,
        val leadId: String?,
        val approvalChannel: String?,
        val raw: JSONObject
    )

    data class CommandHealthSnapshot(
        val queuedCount: Int,
        val lastError: String?,
        val lastFlushSent: Int,
        val lastFlushRemaining: Int,
        val updatedAtMs: Long
    )

    private data class QueuedCommand(
        val commandType: String,
        val correlationId: String?,
        val payload: JSONObject,
        val queuedAt: Long
    )

    private data class SendResult(
        val response: CreateCommandResponse? = null,
        val errorText: String? = null,
        val shouldQueue: Boolean = false
    )

    private fun extractTokenClaim(accessToken: String?, key: String): String {
        if (accessToken.isNullOrBlank()) return ""
        return try {
            val parts = accessToken.split(".")
            if (parts.size < 2) return ""
            val payloadRaw = parts[1]
                .replace('-', '+')
                .replace('_', '/')
            val padded = payloadRaw + "=".repeat((4 - payloadRaw.length % 4) % 4)
            val decoded = String(Base64.decode(padded, Base64.DEFAULT), Charsets.UTF_8)
            JSONObject(decoded).optString(key)
        } catch (_: Exception) {
            ""
        }
    }

    private fun applyUserHeaders(conn: HttpURLConnection, context: Context) {
        val accessToken = AuthStore.getAccessToken(context)
        val userId = AuthStore.getUserId(context)
        val displayName = AuthStore.getDisplayName(context)
        val userEmail = AuthStore.getUserEmail(context)
        val role = AuthStore.getRole(context)
        val dealerId = AuthStore.getDealerId(context)
        val branchId = AuthStore.getBranchId(context)

        accessToken?.let { conn.setRequestProperty("X-User-Token", it) }
        userId?.let { conn.setRequestProperty("X-User-Id", it) }
        displayName?.let { conn.setRequestProperty("X-User-Name", it) }
        userEmail?.let { conn.setRequestProperty("X-User-Email", it) }
        role?.let { conn.setRequestProperty("X-User-Role", it) }

        // Diagnostic log for 401/403 troubleshooting. Never log token value.
        val tokenEmail = extractTokenClaim(accessToken, "email")
        val tokenDealer = extractTokenClaim(accessToken, "dealerId")
        Log.i(
            TAG,
            "tenant_headers userEmail=${userEmail.orEmpty()} dealerId=${dealerId.orEmpty()} branchId=${branchId.orEmpty()} role=${role.orEmpty()} tokenPresent=${!accessToken.isNullOrBlank()} tokenEmail=$tokenEmail tokenDealerId=$tokenDealer"
        )
    }

    private fun healthPrefs(context: Context): SharedPreferences =
        context.getSharedPreferences(HEALTH_PREFS, Context.MODE_PRIVATE)

    private fun saveHealthError(context: Context, message: String?) {
        val editor = healthPrefs(context).edit()
        if (message.isNullOrBlank()) editor.remove(HEALTH_LAST_ERROR) else editor.putString(HEALTH_LAST_ERROR, message)
        editor.putLong(HEALTH_LAST_UPDATE_MS, System.currentTimeMillis())
        editor.apply()
    }

    private fun saveHealthFlush(context: Context, sent: Int, remaining: Int) {
        healthPrefs(context).edit()
            .putInt(HEALTH_LAST_FLUSH_SENT, sent)
            .putInt(HEALTH_LAST_FLUSH_REMAINING, remaining)
            .putLong(HEALTH_LAST_UPDATE_MS, System.currentTimeMillis())
            .apply()
    }

    fun getCommandHealthSnapshot(context: Context): CommandHealthSnapshot {
        val hp = healthPrefs(context)
        return CommandHealthSnapshot(
            queuedCount = getQueuedCommandCount(context),
            lastError = hp.getString(HEALTH_LAST_ERROR, null),
            lastFlushSent = hp.getInt(HEALTH_LAST_FLUSH_SENT, 0),
            lastFlushRemaining = hp.getInt(HEALTH_LAST_FLUSH_REMAINING, 0),
            updatedAtMs = hp.getLong(HEALTH_LAST_UPDATE_MS, 0L)
        )
    }

    private fun normalizeErrorText(raw: String): String {
        val text = raw.trim()
        if (text.isBlank()) return "Request failed. Please try again."
        if (text.contains("Cannot GET", ignoreCase = true) || text.contains("<!DOCTYPE html>", ignoreCase = true)) {
            return "Feature not available on this connector yet. Please deploy latest backend."
        }
        if (text.contains("HTTP 401")) return "Authentication failed. Check API key and sign in again."
        if (text.contains("HTTP 403")) {
            val hint = text.substringAfter("\n\n").trim().lineSequence().firstOrNull()?.trim().orEmpty()
            return if (hint.isNotBlank()) {
                "Access denied: $hint Check USER_EMAIL_DEALER_MAP / dealer id on the connector (Render env)."
            } else {
                "Access denied for this dealer/user. Check tenant mapping and login account."
            }
        }
        if (text.contains("HTTP 404")) return "Requested endpoint or record not found on the connector."
        if (text.contains("HTTP 5")) return "Connector is temporarily unavailable. Command can be retried."
        if (text.contains("timeout", ignoreCase = true)) return "Request timed out. Check network and retry."
        if (text.contains("UnknownHost", ignoreCase = true) || text.contains("failed to connect", ignoreCase = true)) {
            return "Cannot reach connector host. Verify Base URL and network."
        }
        return text.lineSequence().firstOrNull()?.trim().orEmpty().ifBlank { text }
    }


    fun createCommand(
        context: Context,
        commandType: String,
        correlationId: String? = null,
        payload: JSONObject,
        onSuccess: (CreateCommandResponse) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val result = sendCommandBlocking(context, commandType, correlationId, payload)
                if (result.response != null) {
                    saveHealthError(context, null)
                    onSuccess(result.response)
                    return@Thread
                }
                if (result.shouldQueue) {
                    enqueueCommand(context, QueuedCommand(commandType, correlationId, payload, System.currentTimeMillis()))
                    val msg = "No connection right now. Command queued offline and will be retried."
                    saveHealthError(context, msg)
                    onError(msg)
                    return@Thread
                }
                val msg = normalizeErrorText(result.errorText ?: "Unknown error")
                saveHealthError(context, msg)
                onError(msg)
            } catch (e: Exception) {
                Log.e(TAG, "createCommand failed", e)
                enqueueCommand(context, QueuedCommand(commandType, correlationId, payload, System.currentTimeMillis()))
                val msg = "No connection right now. Command queued offline and will be retried."
                saveHealthError(context, msg)
                onError(msg)
            }
        }.start()
    }

    fun getCommandStatus(
        context: Context,
        correlationId: String,
        onSuccess: (CommandStatusResponse) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val baseUrl = ApiConfig.getBaseUrl(context)
                val url = URL("$baseUrl/api/v1/commands/$correlationId")

                val apiKey = ApiConfig.getApiKey(context)
                val auth = "Bearer $apiKey"

                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "GET"
                conn.setRequestProperty("Authorization", auth)
                applyUserHeaders(conn, context)
                conn.connectTimeout = 10_000
                conn.readTimeout = 10_000

                val code = conn.responseCode
                val responseBody = readBody(conn)

                if (code !in 200..299) {
                    onError(normalizeErrorText("HTTP $code: $responseBody"))
                    return@Thread
                }

                val json = JSONObject(responseBody.ifEmpty { "{}" })
                val resultObj = json.optJSONObject("result")
                val resp = CommandStatusResponse(
                    correlationId = json.optString("correlationId", correlationId),
                    status = json.optString("status", ""),
                    commandType = json.optString("commandType", ""),
                    managerSeen = json.optBoolean("managerSeen", false),
                    managerSeenAt = json.optString("managerSeenAt").ifBlank { null },
                    result = resultObj,
                    error = if (json.has("error") && !json.isNull("error")) json.optString("error") else null
                )
                onSuccess(resp)
            } catch (e: Exception) {
                Log.e(TAG, "getCommandStatus failed", e)
                onError(normalizeErrorText(e.message ?: "Unknown error"))
            }
        }.start()
    }

    fun getCommandStatusBlocking(
        context: Context,
        correlationId: String
    ): CommandStatusResponse {
        val baseUrl = ApiConfig.getBaseUrl(context)
        val url = URL("$baseUrl/api/v1/commands/$correlationId")

        val apiKey = ApiConfig.getApiKey(context)
        val auth = "Bearer $apiKey"

        val conn = url.openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.setRequestProperty("Authorization", auth)
        applyUserHeaders(conn, context)
        conn.connectTimeout = 10_000
        conn.readTimeout = 10_000

        val code = conn.responseCode
        val responseBody = readBody(conn)
        if (code !in 200..299) {
            throw IllegalStateException("HTTP $code: $responseBody")
        }

        val json = JSONObject(responseBody.ifEmpty { "{}" })
        return CommandStatusResponse(
            correlationId = json.optString("correlationId", correlationId),
            status = json.optString("status", ""),
            commandType = json.optString("commandType", ""),
            managerSeen = json.optBoolean("managerSeen", false),
            managerSeenAt = json.optString("managerSeenAt").ifBlank { null },
            result = json.optJSONObject("result"),
            error = if (json.has("error") && !json.isNull("error")) json.optString("error") else null
        )
    }

    fun getPendingApprovals(
        context: Context,
        onSuccess: (List<ApprovalItem>) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val baseUrl = ApiConfig.getBaseUrl(context)
                val apiKey = ApiConfig.getApiKey(context)
                val url = URL("$baseUrl/api/v1/approvals?status=pending")
                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "GET"
                conn.setRequestProperty("Authorization", "Bearer $apiKey")
                applyUserHeaders(conn, context)
                conn.connectTimeout = 10_000
                conn.readTimeout = 20_000
                val code = conn.responseCode
                val body = readBody(conn)
                if (code !in 200..299) {
                    onError(normalizeErrorText("HTTP $code\n\n$body"))
                    return@Thread
                }
                val json = JSONObject(body.ifEmpty { "{}" })
                val arr = json.optJSONArray("approvals")
                val list = mutableListOf<ApprovalItem>()
                if (arr != null) {
                    for (i in 0 until arr.length()) {
                        val jo = arr.optJSONObject(i) ?: continue
                        list += ApprovalItem(
                            correlationId = jo.optString("correlationId"),
                            status = jo.optString("status"),
                            commandType = jo.optString("commandType"),
                            requestedCommandType = jo.optString("requestedCommandType").ifBlank { null },
                            createdAt = jo.optString("createdAt"),
                            managerSeen = jo.optBoolean("managerSeen", false),
                            managerSeenAt = jo.optString("managerSeenAt").ifBlank { null },
                            payload = jo.optJSONObject("payload")
                        )
                    }
                }
                onSuccess(list)
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Failed to load approvals"))
            }
        }.start()
    }

    fun approveRequest(
        context: Context,
        correlationId: String,
        onSuccess: () -> Unit,
        onError: (String) -> Unit
    ) {
        postApprovalAction(context, correlationId, "approve", null, onSuccess, onError)
    }

    fun rejectRequest(
        context: Context,
        correlationId: String,
        reason: String,
        onSuccess: () -> Unit,
        onError: (String) -> Unit
    ) {
        postApprovalAction(context, correlationId, "reject", reason, onSuccess, onError)
    }

    fun getStocks(
        context: Context,
        search: String = "",
        refresh: Boolean = false,
        mode: String = "share_stock",
        limit: Int = 100,
        offset: Int = 0,
        onSuccess: (StockListResponse) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val baseUrl = ApiConfig.getBaseUrl(context)
                val apiKey = ApiConfig.getApiKey(context)
                val encodedSearch = java.net.URLEncoder.encode(search, "UTF-8")
                val refreshFlag = if (refresh) 1 else 0
                val encodedMode = java.net.URLEncoder.encode(mode, "UTF-8")
                val url = URL("$baseUrl/api/v1/stocks?limit=$limit&offset=$offset&search=$encodedSearch&refresh=$refreshFlag&mode=$encodedMode")
                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "GET"
                conn.setRequestProperty("Authorization", "Bearer $apiKey")
                applyUserHeaders(conn, context)
                AuthStore.getDealerId(context)?.let { conn.setRequestProperty("X-Dealer-Id", it) }
                AuthStore.getBranchId(context)?.let { conn.setRequestProperty("X-Branch-Id", it) }
                conn.connectTimeout = 10_000
                conn.readTimeout = 25_000
                val code = conn.responseCode
                val body = readBody(conn)
                if (code !in 200..299) {
                    onError(normalizeErrorText("HTTP $code\n\n$body"))
                    return@Thread
                }
                val json = JSONObject(body.ifEmpty { "{}" })
                val arr = json.optJSONArray("stocks")
                val list = mutableListOf<StockItem>()
                if (arr != null) {
                    for (i in 0 until arr.length()) {
                        val jo = arr.optJSONObject(i) ?: continue
                        list += StockItem(
                            stockNumber = jo.optString("stockNumber"),
                            registrationNumber = jo.optString("registrationNumber"),
                            make = jo.optString("make"),
                            model = jo.optString("model"),
                            variant = jo.optString("variant"),
                            year = jo.optString("year"),
                            price = jo.optString("price"),
                            autoTraderPrice = jo.optString("autoTraderPrice").ifBlank { jo.optString("price") },
                            tradePrice = jo.optString("tradePrice"),
                            retailPrice = jo.optString("retailPrice"),
                            marketPrice = jo.optString("marketPrice"),
                            valuationStatus = jo.optString("valuationStatus"),
                            primaryImageUrl = jo.optString("primaryImageUrl").ifBlank { null },
                            raw = jo.optJSONObject("raw")
                        )
                    }
                }
                onSuccess(
                    StockListResponse(
                        stocks = list,
                        cachedAt = json.optString("cachedAt").ifBlank { null },
                        source = json.optString("source").ifBlank { null },
                        total = json.optInt("total", list.size),
                        provider = json.optString("provider").ifBlank { null },
                        dealerScope = json.optString("dealerScope").ifBlank { null },
                        warning = json.optString("warning").ifBlank { null }
                    )
                )
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Failed to load stocks"))
            }
        }.start()
    }

    fun getQueuedCommandCount(context: Context): Int = loadQueue(context).size

    fun createConsent(
        context: Context,
        payload: JSONObject,
        onSuccess: (ConsentStatusResponse) -> Unit,
        onError: (String) -> Unit
    ) = postJson(
        context = context,
        path = "/api/v1/consents",
        body = payload,
        onSuccess = { json ->
            try {
                onSuccess(parseConsentStatus(json))
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Invalid consent response"))
            }
        },
        onError = onError,
        readTimeoutMs = READ_TIMEOUT_MS_CONSENT
    )

    fun getConsentStatus(
        context: Context,
        consentId: String,
        onSuccess: (ConsentStatusResponse) -> Unit,
        onError: (String) -> Unit
    ) = getJson(
        context = context,
        path = "/api/v1/consents/$consentId",
        onSuccess = { json ->
            try {
                onSuccess(parseConsentStatus(json))
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Invalid consent response"))
            }
        },
        onError = onError
    )

    fun revokeConsent(
        context: Context,
        consentId: String,
        reason: String?,
        onSuccess: (ConsentStatusResponse) -> Unit,
        onError: (String) -> Unit
    ) {
        val body = JSONObject().apply {
            if (!reason.isNullOrBlank()) put("reason", reason.trim())
        }
        postJson(
            context = context,
            path = "/api/v1/consents/$consentId/revoke",
            body = body,
            onSuccess = { json ->
                try {
                    onSuccess(parseConsentStatus(json))
                } catch (e: Exception) {
                    onError(normalizeErrorText(e.message ?: "Invalid consent response"))
                }
            },
            onError = onError,
            readTimeoutMs = READ_TIMEOUT_MS_CONSENT
        )
    }

    fun getKpis(
        context: Context,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = getJson(context, "/api/v1/analytics/kpis", onSuccess, onError)

    fun getForecast(
        context: Context,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = getJson(context, "/api/v1/analytics/forecast", onSuccess, onError)

    fun getOemRollup(
        context: Context,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = getJson(context, "/api/v1/analytics/oem-rollup", onSuccess, onError)

    fun saveTargets(
        context: Context,
        targets: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = putJson(context, "/api/v1/analytics/targets", targets, onSuccess, onError)

    fun startTestDrive(
        context: Context,
        payload: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = postJson(context, "/api/v1/test-drives/start", payload, onSuccess, onError)

    fun testDriveCheckin(
        context: Context,
        sessionId: String,
        payload: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = postJson(context, "/api/v1/test-drives/$sessionId/checkin", payload, onSuccess, onError)

    fun completeTestDrive(
        context: Context,
        sessionId: String,
        payload: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = postJson(context, "/api/v1/test-drives/$sessionId/complete", payload, onSuccess, onError)

    fun getActiveTestDrives(
        context: Context,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) = getJson(context, "/api/v1/test-drives/active", onSuccess, onError)

    fun flushQueuedCommands(
        context: Context,
        onProgress: ((remaining: Int) -> Unit)? = null,
        onDone: ((sentCount: Int, remaining: Int) -> Unit)? = null
    ) {
        Thread {
            val queue = loadQueue(context).toMutableList()
            if (queue.isEmpty()) {
                saveHealthFlush(context, 0, 0)
                onDone?.invoke(0, 0)
                return@Thread
            }
            var sent = 0
            val iterator = queue.iterator()
            while (iterator.hasNext()) {
                val cmd = iterator.next()
                val result = sendCommandBlocking(context, cmd.commandType, cmd.correlationId, cmd.payload)
                if (result.response != null) {
                    iterator.remove()
                    sent += 1
                    saveQueue(context, queue)
                    onProgress?.invoke(queue.size)
                    continue
                }
                // Keep queued on network errors; stop early to avoid hammering.
                if (result.shouldQueue) break
                // Permanent errors get dropped to avoid retry loops.
                iterator.remove()
                saveQueue(context, queue)
                onProgress?.invoke(queue.size)
            }
            saveHealthFlush(context, sent, queue.size)
            if (queue.isEmpty()) saveHealthError(context, null)
            onDone?.invoke(sent, queue.size)
        }.start()
    }

    private fun sendCommandBlocking(
        context: Context,
        commandType: String,
        correlationId: String?,
        payload: JSONObject
    ): SendResult {
        return try {
            val baseUrl = ApiConfig.getBaseUrl(context)
            val url = URL("$baseUrl/api/v1/commands")
            val apiKey = ApiConfig.getApiKey(context)
            val auth = "Bearer $apiKey"
            val body = JSONObject().apply {
                put("commandType", commandType)
                if (!correlationId.isNullOrBlank()) put("correlationId", correlationId)
                put("payload", payload)
            }
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", auth)
            applyUserHeaders(conn, context)
            AuthStore.getDealerId(context)?.let { conn.setRequestProperty("X-Dealer-Id", it) }
            AuthStore.getBranchId(context)?.let { conn.setRequestProperty("X-Branch-Id", it) }
            conn.doOutput = true
            conn.connectTimeout = 15_000
            conn.readTimeout = 60_000
            conn.outputStream.use { os ->
                os.write(body.toString().toByteArray(Charsets.UTF_8))
            }
            val code = conn.responseCode
            val responseBody = readBody(conn)
            if (code == 401) {
                return SendResult(errorText = context.getString(R.string.api_key_rejected_message))
            }
            if (code != 202 && code != 200) {
                val detail = try {
                    val jo = JSONObject(responseBody.ifEmpty { "{}" })
                    val err = jo.optString("error", "").trim()
                    val hint = jo.optString("hint", "").trim()
                    listOf(err, hint).filter { it.isNotEmpty() }.joinToString(" — ").ifBlank { responseBody }
                } catch (_: Exception) {
                    responseBody
                }
                val shouldQueue = code in 500..599
                return SendResult(errorText = "HTTP $code\n\n${detail.ifBlank { "(empty body — check connector terminal on PC)" }}", shouldQueue = shouldQueue)
            }
            val json = JSONObject(responseBody.ifEmpty { "{}" })
            SendResult(
                response = CreateCommandResponse(
                    correlationId = json.optString("correlationId", correlationId ?: ""),
                    status = json.optString("status", "queued"),
                    commandType = json.optString("commandType", commandType),
                    result = json.optJSONObject("result")
                )
            )
        } catch (e: Exception) {
            val networkish = e is SocketTimeoutException || e is java.net.ConnectException || e is java.net.UnknownHostException
            SendResult(errorText = e.message ?: "Unknown error", shouldQueue = networkish)
        }
    }

    private fun queuePrefs(context: Context): SharedPreferences =
        context.getSharedPreferences(QUEUE_PREFS, Context.MODE_PRIVATE)

    private fun loadQueue(context: Context): List<QueuedCommand> {
        val raw = queuePrefs(context).getString(QUEUE_KEY, "[]").orEmpty()
        return try {
            val arr = JSONArray(raw)
            val out = mutableListOf<QueuedCommand>()
            for (i in 0 until arr.length()) {
                val jo = arr.optJSONObject(i) ?: continue
                out += QueuedCommand(
                    commandType = jo.optString("commandType"),
                    correlationId = jo.optString("correlationId").ifBlank { null },
                    payload = jo.optJSONObject("payload") ?: JSONObject(),
                    queuedAt = jo.optLong("queuedAt", System.currentTimeMillis())
                )
            }
            out
        } catch (_: Exception) {
            emptyList()
        }
    }

    private fun saveQueue(context: Context, items: List<QueuedCommand>) {
        val arr = JSONArray()
        items.forEach { item ->
            arr.put(
                JSONObject().apply {
                    put("commandType", item.commandType)
                    put("correlationId", item.correlationId)
                    put("payload", item.payload)
                    put("queuedAt", item.queuedAt)
                }
            )
        }
        queuePrefs(context).edit().putString(QUEUE_KEY, arr.toString()).apply()
    }

    private fun enqueueCommand(context: Context, cmd: QueuedCommand) {
        val items = loadQueue(context).toMutableList()
        items += cmd
        saveQueue(context, items)
    }

    private fun postApprovalAction(
        context: Context,
        correlationId: String,
        action: String,
        reason: String?,
        onSuccess: () -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val baseUrl = ApiConfig.getBaseUrl(context)
                val apiKey = ApiConfig.getApiKey(context)
                val url = URL("$baseUrl/api/v1/approvals/$correlationId/$action")
                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "POST"
                conn.setRequestProperty("Authorization", "Bearer $apiKey")
                conn.setRequestProperty("Content-Type", "application/json")
                applyUserHeaders(conn, context)
                conn.doOutput = true
                val body = JSONObject().apply {
                    if (!reason.isNullOrBlank()) put("reason", reason)
                }
                conn.outputStream.use { it.write(body.toString().toByteArray(Charsets.UTF_8)) }
                val code = conn.responseCode
                val resp = readBody(conn)
                if (code !in 200..299) {
                    onError(normalizeErrorText("HTTP $code\n\n$resp"))
                    return@Thread
                }
                onSuccess()
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Action failed"))
            }
        }.start()
    }

    private fun getJson(
        context: Context,
        path: String,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val json = requestJson(context, "GET", path, null, 20_000)
                onSuccess(json)
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Request failed"))
            }
        }.start()
    }

    private fun postJson(
        context: Context,
        path: String,
        body: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit,
        readTimeoutMs: Int = 20_000
    ) {
        Thread {
            try {
                val json = requestJson(context, "POST", path, body, readTimeoutMs)
                onSuccess(json)
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Request failed"))
            }
        }.start()
    }

    private fun putJson(
        context: Context,
        path: String,
        body: JSONObject,
        onSuccess: (JSONObject) -> Unit,
        onError: (String) -> Unit
    ) {
        Thread {
            try {
                val json = requestJson(context, "PUT", path, body, 20_000)
                onSuccess(json)
            } catch (e: Exception) {
                onError(normalizeErrorText(e.message ?: "Request failed"))
            }
        }.start()
    }

    private fun requestJson(
        context: Context,
        method: String,
        path: String,
        body: JSONObject?,
        readTimeoutMs: Int = 20_000
    ): JSONObject {
        val baseUrl = ApiConfig.getBaseUrl(context)
        val apiKey = ApiConfig.getApiKey(context)
        val url = URL("$baseUrl$path")
        val conn = url.openConnection() as HttpURLConnection
        conn.requestMethod = method
        conn.setRequestProperty("Authorization", "Bearer $apiKey")
        conn.setRequestProperty("Content-Type", "application/json")
        applyUserHeaders(conn, context)
        AuthStore.getDealerId(context)?.let { conn.setRequestProperty("X-Dealer-Id", it) }
        AuthStore.getBranchId(context)?.let { conn.setRequestProperty("X-Branch-Id", it) }
        conn.connectTimeout = 10_000
        conn.readTimeout = readTimeoutMs
        if (body != null) {
            conn.doOutput = true
            conn.outputStream.use { it.write(body.toString().toByteArray(Charsets.UTF_8)) }
        }
        val code = conn.responseCode
        val resp = readBody(conn)
        if (code !in 200..299) throw IllegalStateException("HTTP $code: $resp")
        return JSONObject(resp.ifEmpty { "{}" })
    }

    private fun parseConsentStatus(json: JSONObject): ConsentStatusResponse {
        return ConsentStatusResponse(
            consentId = json.optString("consentId").ifBlank { json.optString("id") },
            status = json.optString("status"),
            purpose = json.optString("purpose").ifBlank { null },
            noticeVersion = json.optString("noticeVersion").ifBlank { null },
            requestedAt = json.optString("requestedAt").ifBlank { null },
            expiresAt = json.optString("expiresAt").ifBlank { null },
            approvedAt = json.optString("approvedAt").ifBlank { null },
            rejectedAt = json.optString("rejectedAt").ifBlank { null },
            revokedAt = json.optString("revokedAt").ifBlank { null },
            leadCorrelationId = json.optString("leadCorrelationId").ifBlank { null },
            leadId = json.optString("leadId").ifBlank { null },
            approvalChannel = json.optString("approvalChannel").ifBlank { null },
            raw = json
        )
    }


    private fun readBody(conn: HttpURLConnection): String {
        return try {
            val stream = if (conn.responseCode in 200..299) conn.inputStream else conn.errorStream
            BufferedReader(InputStreamReader(stream)).use { it.readText() }
        } catch (_: Exception) {
            ""
        }
    }
}

