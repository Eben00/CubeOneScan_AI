package com.cubeone.scan.core.auth

import android.content.Context
import android.util.Base64
import android.util.Log
import com.cubeone.scan.core.api.ApiClient
import com.cubeone.scan.services.ApiConfig
import org.json.JSONObject
import org.json.JSONArray
import java.io.OutputStreamWriter
import java.net.URLEncoder
import java.net.InetSocketAddress
import java.net.URI
import java.net.Socket
import java.net.HttpURLConnection
import java.net.URL

object AuthApiService {

    private const val TAG = "AuthApiService"

    /** Unverified decode of JWT payload string claim (issuer signs the token; connector uses the same dealerId claim). */
    private fun jwtPayloadStringClaim(jwt: String, claim: String): String? {
        val parts = jwt.split(".")
        if (parts.size < 2) return null
        return try {
            var segment = parts[1].replace('-', '+').replace('_', '/')
            when (segment.length % 4) {
                2 -> segment += "=="
                3 -> segment += "="
            }
            val json = String(Base64.decode(segment, Base64.DEFAULT), Charsets.UTF_8)
            JSONObject(json).optString(claim).trim().takeIf { it.isNotEmpty() }
        } catch (_: Exception) {
            null
        }
    }

    private const val DEFAULT_AUTH_BASE_URL = "http://10.0.2.2:4000"

    private val client = ApiClient()

    fun login(context: Context, email: String, password: String): AuthResult {
        return try {
            val body = JSONObject().apply {
                put("username", email)
                put("password", password)
            }
            val url = "${resolveAuthBaseUrl(context)}/api/v1/auth/login"
            val response = client.postDetailed(url, body.toString())
            when (response.statusCode) {
                200 -> {
                    val json = JSONObject(response.body.orEmpty())
                    val accessToken = json.optString("access_token")
                    val refreshToken = json.optString("refresh_token", null)
                    val user = json.optJSONObject("user")
                    if (accessToken.isNullOrBlank()) {
                        AuthResult.Error("No access token returned")
                    } else {
                        val jwtDealer = jwtPayloadStringClaim(accessToken, "dealerId")
                        val apiDealer = user?.optString("dealerId")?.trim()?.takeIf { it.isNotEmpty() }
                        AuthResult.Success(
                            accessToken = accessToken,
                            refreshToken = refreshToken,
                            userId = user?.optString("userId"),
                            dealerId = jwtDealer ?: apiDealer,
                            branchId = user?.optString("branchId"),
                            role = user?.optString("role"),
                            mustChangePassword = user?.optBoolean("mustChangePassword", false) == true
                        )
                    }
                }
                401 -> AuthResult.Error("Invalid email or password")
                423 -> AuthResult.Error("Account locked due to failed attempts. Try again later.")
                -1 -> AuthResult.Error("Login failed (cannot reach auth server)")
                else -> AuthResult.Error(
                    "Login failed (HTTP ${response.statusCode})" +
                        (if (!response.body.isNullOrBlank()) ": ${response.body}" else "")
                )
            }
        } catch (e: Exception) {
            Log.e(TAG, "Login error", e)
            AuthResult.Error(e.message ?: "Unexpected error")
        }
    }

    fun register(context: Context, email: String, password: String): RegisterResult {
        return try {
            val body = JSONObject().apply {
                put("email", email)
                put("password", password)
                put("dealerId", resolveDealerId(context))
                put("branchId", "branch_main")
            }
            val url = "${resolveAuthBaseUrl(context)}/api/v1/auth/register"
            val response = client.postDetailed(url, body.toString())
            when (response.statusCode) {
                200, 201 -> RegisterResult.Success
                409 -> RegisterResult.Error("Account already exists. Please sign in.")
                -1 -> RegisterResult.Error("Cannot reach auth server on port 4000")
                else -> RegisterResult.Error(
                    "Registration failed (HTTP ${response.statusCode})" +
                        (if (!response.body.isNullOrBlank()) ": ${response.body}" else "")
                )
            }
        } catch (e: Exception) {
            Log.e(TAG, "Register error", e)
            RegisterResult.Error(e.message ?: "Registration failed")
        }
    }

    fun listAdminUsers(context: Context): AdminUsersResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminUsersResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "GET"
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val body = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) {
                return AdminUsersResult.Error("Load users failed (HTTP $code): $body")
            }
            val json = JSONObject(body.ifBlank { "{}" })
            val arr = json.optJSONArray("users") ?: JSONArray()
            val list = mutableListOf<AdminUser>()
            for (i in 0 until arr.length()) {
                val u = arr.optJSONObject(i) ?: continue
                list += AdminUser(
                    userId = u.optString("userId"),
                    email = u.optString("email"),
                    dealerId = u.optString("dealerId"),
                    branchId = u.optString("branchId"),
                    role = u.optString("role"),
                    active = u.optBoolean("active", true),
                    lockedUntil = u.optString("lockedUntil", null)
                )
            }
            AdminUsersResult.Success(list)
        } catch (e: Exception) {
            AdminUsersResult.Error(e.message ?: "Failed to load users")
        }
    }

    fun createAdminUser(
        context: Context,
        email: String,
        password: String,
        role: String
    ): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val body = JSONObject().apply {
                put("email", email)
                put("password", password)
                put("dealerId", AuthStore.getDealerId(context).orEmpty())
                put("branchId", AuthStore.getBranchId(context).orEmpty())
                put("role", role)
            }.toString()
            OutputStreamWriter(conn.outputStream).use { it.write(body) }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Create user failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to create user")
        }
    }

    fun updateUserRole(context: Context, userId: String, role: String): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users/$userId/role")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "PATCH"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val body = JSONObject().apply { put("role", role) }.toString()
            OutputStreamWriter(conn.outputStream).use { it.write(body) }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Update role failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to update role")
        }
    }

    fun updateUserStatus(context: Context, userId: String, active: Boolean): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users/$userId/status")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "PATCH"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val body = JSONObject().apply { put("active", active) }.toString()
            OutputStreamWriter(conn.outputStream).use { it.write(body) }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Update status failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to update status")
        }
    }

    fun resetUserPassword(context: Context, userId: String, newPassword: String): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users/$userId/reset-password")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val body = JSONObject().apply { put("newPassword", newPassword) }.toString()
            OutputStreamWriter(conn.outputStream).use { it.write(body) }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Reset password failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to reset password")
        }
    }

    fun unlockUser(context: Context, userId: String): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/users/$userId/unlock")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            OutputStreamWriter(conn.outputStream).use { it.write("{}") }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Unlock failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to unlock user")
        }
    }

    fun changeOwnPassword(context: Context, currentPassword: String, newPassword: String): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/auth/change-password")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val body = JSONObject().apply {
                put("currentPassword", currentPassword)
                put("newPassword", newPassword)
            }.toString()
            OutputStreamWriter(conn.outputStream).use { it.write(body) }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Change password failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to change password")
        }
    }

    fun logoutCurrentSession(context: Context): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/auth/logout")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            OutputStreamWriter(conn.outputStream).use { it.write("{}") }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Logout failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to logout")
        }
    }

    fun logoutAllSessions(context: Context): AdminActionResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AdminActionResult.Error("Not logged in")
        return try {
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/auth/logout-all")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "POST"
            conn.doOutput = true
            conn.setRequestProperty("Content-Type", "application/json")
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            OutputStreamWriter(conn.outputStream).use { it.write("{}") }
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val resp = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AdminActionResult.Error("Logout all failed (HTTP $code): $resp")
            AdminActionResult.Success
        } catch (e: Exception) {
            AdminActionResult.Error(e.message ?: "Failed to logout all")
        }
    }

    fun listAuditEvents(
        context: Context,
        actionFilter: String = "",
        userFilter: String = "",
        limit: Int = 200
    ): AuditEventsResult {
        val token = AuthStore.getAccessToken(context).orEmpty()
        if (token.isBlank()) return AuditEventsResult.Error("Not logged in")
        return try {
            val query = buildString {
                append("limit=${limit.coerceIn(1, 500)}")
                if (actionFilter.isNotBlank()) {
                    append("&action=${URLEncoder.encode(actionFilter, "UTF-8")}")
                }
                if (userFilter.isNotBlank()) {
                    append("&user=${URLEncoder.encode(userFilter, "UTF-8")}")
                }
            }
            val url = URL("${resolveAuthBaseUrl(context)}/api/v1/admin/audit-events?$query")
            val conn = url.openConnection() as HttpURLConnection
            conn.requestMethod = "GET"
            conn.setRequestProperty("Authorization", "Bearer $token")
            conn.connectTimeout = 20_000
            conn.readTimeout = 20_000
            val code = conn.responseCode
            val stream = if (code in 200..299) conn.inputStream else conn.errorStream
            val body = stream?.bufferedReader()?.use { it.readText() }.orEmpty()
            if (code !in 200..299) return AuditEventsResult.Error("Load audit events failed (HTTP $code): $body")
            val json = JSONObject(body.ifBlank { "{}" })
            val arr = json.optJSONArray("events") ?: JSONArray()
            val out = mutableListOf<AuditEvent>()
            for (i in 0 until arr.length()) {
                val evt = arr.optJSONObject(i) ?: continue
                out += AuditEvent(
                    id = evt.optString("id"),
                    ts = evt.optString("ts"),
                    type = evt.optString("type"),
                    actorEmail = evt.optString("actorEmail"),
                    targetEmail = evt.optString("targetEmail"),
                    dealerId = evt.optString("dealerId")
                )
            }
            AuditEventsResult.Success(out)
        } catch (e: Exception) {
            AuditEventsResult.Error(e.message ?: "Failed to load audit events")
        }
    }

    fun getAuthBaseUrl(context: Context): String = resolveAuthBaseUrl(context)

    fun isAuthServerReachable(context: Context): Boolean {
        return try {
            val uri = URI(resolveAuthBaseUrl(context))
            val host = uri.host ?: return false
            val port = if (uri.port != -1) uri.port else if (uri.scheme.equals("https", true)) 443 else 80
            Socket().use { socket ->
                socket.connect(InetSocketAddress(host, port), 2000)
            }
            true
        } catch (_: Exception) {
            false
        }
    }

    private fun resolveAuthBaseUrl(context: Context): String {
        val authOverride = ApiConfig.getAuthBaseUrl(context)
        if (authOverride.isNotBlank()) return authOverride
        val connectorBase = ApiConfig.getBaseUrl(context)
        if (connectorBase.isBlank()) return DEFAULT_AUTH_BASE_URL
        return connectorBase.replace(Regex(":\\d+($|/)"), ":4000$1")
    }

    private fun resolveDealerId(context: Context): String {
        return try {
            val base = ApiConfig.getBaseUrl(context)
            if (base.isBlank()) return "dealer_default"
            val host = URI(base).host.orEmpty().lowercase()
            if (host.isBlank()) return "dealer_default"
            "dealer_${host.replace(Regex("[^a-z0-9]+"), "_").trim('_')}"
        } catch (_: Exception) {
            "dealer_default"
        }
    }
}

sealed class AuthResult {
    data class Success(
        val accessToken: String,
        val refreshToken: String?,
        val userId: String?,
        val dealerId: String?,
        val branchId: String?,
        val role: String?,
        val mustChangePassword: Boolean = false
    ) : AuthResult()
    data class Error(val message: String) : AuthResult()
}

sealed class RegisterResult {
    object Success : RegisterResult()
    data class Error(val message: String) : RegisterResult()
}

data class AdminUser(
    val userId: String,
    val email: String,
    val dealerId: String,
    val branchId: String,
    val role: String,
    val active: Boolean,
    val lockedUntil: String?
)

sealed class AdminUsersResult {
    data class Success(val users: List<AdminUser>) : AdminUsersResult()
    data class Error(val message: String) : AdminUsersResult()
}

sealed class AdminActionResult {
    object Success : AdminActionResult()
    data class Error(val message: String) : AdminActionResult()
}

data class AuditEvent(
    val id: String,
    val ts: String,
    val type: String,
    val actorEmail: String,
    val targetEmail: String,
    val dealerId: String
)

sealed class AuditEventsResult {
    data class Success(val events: List<AuditEvent>) : AuditEventsResult()
    data class Error(val message: String) : AuditEventsResult()
}

