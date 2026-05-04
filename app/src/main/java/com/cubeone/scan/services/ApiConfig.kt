package com.cubeone.scan.services

import android.content.Context
import android.content.SharedPreferences
import com.cubeone.scan.BuildConfig
import com.cubeone.scan.R

object ApiConfig {
    private const val PREFS_NAME = "cubeone_connector_prefs"
    private const val KEY_BASE_URL = "base_url"
    private const val KEY_API_KEY = "api_key"
    private const val KEY_AUTH_BASE_URL = "auth_base_url"

    private const val DEFAULT_BASE_URL = ""
    private const val DEFAULT_API_KEY = ""

    private fun prefs(context: Context): SharedPreferences =
        context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

    /** Dealer / pilot APK: URLs and API key baked in at release build; not shown in Settings. */
    private fun lockedConnectorConfig(): Boolean = BuildConfig.LOCK_CONNECTOR_CONFIG

    fun getBaseUrl(context: Context): String {
        if (lockedConnectorConfig() && BuildConfig.LOCKED_CONNECTOR_BASE_URL.isNotBlank()) {
            return normalizeBaseUrl(BuildConfig.LOCKED_CONNECTOR_BASE_URL)
        }
        val saved = prefs(context).getString(KEY_BASE_URL, DEFAULT_BASE_URL) ?: DEFAULT_BASE_URL
        val raw = saved.trim().ifBlank { context.getString(R.string.default_connector_base_url).trim() }
        return normalizeBaseUrl(raw)
    }

    fun getApiKey(context: Context): String {
        if (lockedConnectorConfig() && BuildConfig.LOCKED_CONNECTOR_API_KEY.isNotBlank()) {
            return BuildConfig.LOCKED_CONNECTOR_API_KEY
        }
        return prefs(context).getString(KEY_API_KEY, DEFAULT_API_KEY) ?: DEFAULT_API_KEY
    }

    /**
     * Optional separate auth API origin (e.g. https://auth.example.com).
     * When blank, [com.cubeone.scan.core.auth.AuthApiService] derives auth from the connector URL (port 4000).
     */
    fun getAuthBaseUrl(context: Context): String {
        if (lockedConnectorConfig() && BuildConfig.LOCKED_AUTH_BASE_URL.isNotBlank()) {
            return normalizeBaseUrl(BuildConfig.LOCKED_AUTH_BASE_URL)
        }
        val saved = prefs(context).getString(KEY_AUTH_BASE_URL, "") ?: ""
        val raw = saved.trim().ifBlank { context.getString(R.string.default_auth_base_url).trim() }
        return normalizeBaseUrl(raw)
    }

    fun setConfig(context: Context, baseUrl: String, apiKey: String, authBaseUrl: String? = null) {
        if (lockedConnectorConfig()) return
        val editor = prefs(context).edit()
            .putString(KEY_BASE_URL, normalizeBaseUrl(baseUrl))
            .putString(KEY_API_KEY, apiKey.trim())
        val authNorm = authBaseUrl?.trim().orEmpty().let { if (it.isBlank()) "" else normalizeBaseUrl(it) }
        if (authNorm.isBlank()) {
            editor.remove(KEY_AUTH_BASE_URL)
        } else {
            editor.putString(KEY_AUTH_BASE_URL, authNorm)
        }
        editor.apply()
    }

    fun isValidApiKey(apiKey: String): Boolean {
        val key = apiKey.trim()
        if (key.isBlank()) return false
        if (key.equals("change-me", ignoreCase = true)) return false
        return key.length >= 12
    }

    fun isAllowedBaseUrl(input: String): Boolean {
        val normalized = normalizeBaseUrl(input)
        if (normalized.isBlank()) return false
        val lower = normalized.lowercase()
        return lower.startsWith("https://") || isLocalDevHttp(lower)
    }

    /** Blank is allowed (means “use derived auth URL”). */
    fun isAllowedAuthBaseUrl(input: String): Boolean {
        val t = input.trim()
        if (t.isBlank()) return true
        return isAllowedBaseUrl(input)
    }

    /** Same rules as saved config — use for Test before Save. */
    fun normalizeConnectorBaseUrl(input: String): String = normalizeBaseUrl(input)

    private fun normalizeBaseUrl(input: String): String {
        var s = input.trim().removeSuffix("/")
        if (s.isBlank()) return ""
        // Common mistake: 192.168.1.10.8080 (dot before port) — must be http://192.168.1.10:8080
        val dotPort = Regex(
            "^(https?://)?((?:\\d{1,3}\\.){3}\\d{1,3})\\.(\\d{2,5})$",
            RegexOption.IGNORE_CASE
        )
        dotPort.find(s)?.let { m ->
            val port = m.groupValues[3].toIntOrNull()
            if (port != null && port in 1..65535) {
                val host = m.groupValues[2]
                val https = m.groupValues[1].startsWith("https", ignoreCase = true)
                s = "${if (https) "https" else "http"}://$host:$port"
            }
        }
        if (!s.startsWith("http://", ignoreCase = true) && !s.startsWith("https://", ignoreCase = true)) {
            s = "http://$s"
        }
        return s.removeSuffix("/")
    }

    private fun isLocalDevHttp(url: String): Boolean {
        if (!url.startsWith("http://")) return false
        val private172 = Regex("^http://172\\.(1[6-9]|2\\d|3[0-1])\\..*")
        return url.startsWith("http://10.0.2.2") ||
            url.startsWith("http://127.0.0.1") ||
            url.startsWith("http://localhost") ||
            url.startsWith("http://192.168.") ||
            url.startsWith("http://10.") ||
            private172.matches(url)
    }
}

