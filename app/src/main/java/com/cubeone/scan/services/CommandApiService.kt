package com.cubeone.scan.services

import android.content.Context
import android.content.SharedPreferences
import android.util.Log
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
    private const val QUEUE_PREFS = "command_api_queue"
    private const val QUEUE_KEY = "queued_commands"

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

    private fun applyUserHeaders(conn: HttpURLConnection, context: Context) {
        AuthStore.getAccessToken(context)?.let { conn.setRequestProperty("X-User-Token", it) }
        AuthStore.getUserId(context)?.let { conn.setRequestProperty("X-User-Id", it) }
        AuthStore.getDisplayName(context)?.let { conn.setRequestProperty("X-User-Name", it) }
        AuthStore.getUserEmail(context)?.let { conn.setRequestProperty("X-User-Email", it) }
        AuthStore.getRole(context)?.let { conn.setRequestProperty("X-User-Role", it) }
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
                    onSuccess(result.response)
                    return@Thread
                }
                if (result.shouldQueue) {
                    enqueueCommand(context, QueuedCommand(commandType, correlationId, payload, System.currentTimeMillis()))
                    onError("No connection right now. Command queued offline and will be retried.")
                    return@Thread
                }
                onError(result.errorText ?: "Unknown error")
            } catch (e: Exception) {
                Log.e(TAG, "createCommand failed", e)
                enqueueCommand(context, QueuedCommand(commandType, correlationId, payload, System.currentTimeMillis()))
                onError("No connection right now. Command queued offline and will be retried.")
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
                    onError("HTTP $code: $responseBody")
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
                onError(e.message ?: "Unknown error")
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
                    onError("HTTP $code\n\n$body")
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
                onError(e.message ?: "Failed to load approvals")
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
                    onError("HTTP $code\n\n$body")
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
                onError(e.message ?: "Failed to load stocks")
            }
        }.start()
    }

    fun getQueuedCommandCount(context: Context): Int = loadQueue(context).size

    fun flushQueuedCommands(
        context: Context,
        onProgress: ((remaining: Int) -> Unit)? = null,
        onDone: ((sentCount: Int, remaining: Int) -> Unit)? = null
    ) {
        Thread {
            val queue = loadQueue(context).toMutableList()
            if (queue.isEmpty()) {
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
                return SendResult(errorText = "API key rejected (401). In CubeOneScan Settings, set API Key exactly to API_KEY in connector .env on your PC.")
            }
            if (code != 202 && code != 200) {
                val detail = try {
                    val jo = JSONObject(responseBody.ifEmpty { "{}" })
                    jo.optString("error", "").ifBlank { responseBody }
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
                    onError("HTTP $code\n\n$resp")
                    return@Thread
                }
                onSuccess()
            } catch (e: Exception) {
                onError(e.message ?: "Action failed")
            }
        }.start()
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

