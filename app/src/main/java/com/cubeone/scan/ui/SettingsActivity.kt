package com.cubeone.scan.ui

import android.content.Intent
import android.content.pm.ApplicationInfo
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.Button
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.BuildConfig
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.R
import com.cubeone.scan.services.ApiConfig
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL

class SettingsActivity : AppCompatActivity() {
    private val privacyPolicyUrl by lazy { getString(R.string.settings_privacy_policy_url) }
    private val supportEmail by lazy { getString(R.string.settings_support_email) }
    private val deletionEmail by lazy { getString(R.string.settings_deletion_email) }
    private data class PreflightCheck(val name: String, val ok: Boolean, val detail: String)

    private fun normalizeBusinessRole(rawRole: String): String {
        val role = rawRole.trim().lowercase()
        return when (role) {
            "dealer_principal", "sales_manager", "sales_person" -> role
            "superadmin", "owner", "admin" -> "dealer_principal"
            "agent" -> "sales_person"
            else -> "sales_person"
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val role = normalizeBusinessRole(AuthStore.getRole(this).orEmpty())
        val token = AuthStore.getAccessToken(this).orEmpty()
        val isOfflineDemo = token == "offline-demo-token"
        val canManageUsers = isOfflineDemo || role == "dealer_principal" || role == "sales_manager"
        val canViewAudit = isOfflineDemo || role == "dealer_principal" || role == "sales_manager"
        setContentView(R.layout.activity_settings)

        if (BuildConfig.LOCK_CONNECTOR_CONFIG) {
            findViewById<View>(R.id.groupConnectorSecrets).visibility = View.GONE
            findViewById<TextView>(R.id.tvConnectorManagedNotice).visibility = View.VISIBLE
            findViewById<TextView>(R.id.tvSettingsIntro).text =
                getString(R.string.settings_intro_connector_managed)
        }

        val etBaseUrl = findViewById<EditText>(R.id.etBaseUrl)
        val etAuthBaseUrl = findViewById<EditText>(R.id.etAuthBaseUrl)
        val etApiKey = findViewById<EditText>(R.id.etApiKey)
        val btnSave = findViewById<Button>(R.id.btnSave)
        val btnTest = findViewById<Button>(R.id.btnTest)
        val btnManageUsers = findViewById<Button>(R.id.btnManageUsers)
        val btnAuditEvents = findViewById<Button>(R.id.btnAuditEvents)
        val btnPreflight = findViewById<Button>(R.id.btnPreflight)
        val btnPrivacyPolicy = findViewById<Button>(R.id.btnPrivacyPolicy)
        val btnContactSupport = findViewById<Button>(R.id.btnContactSupport)
        val btnRequestDeletion = findViewById<Button>(R.id.btnRequestDeletion)

        etBaseUrl.setText(ApiConfig.getBaseUrl(this))
        etAuthBaseUrl.setText(ApiConfig.getAuthBaseUrl(this))
        etApiKey.setText(ApiConfig.getApiKey(this))

        btnSave.setOnClickListener {
            val baseUrl = etBaseUrl.text?.toString().orEmpty()
            val authBaseUrl = etAuthBaseUrl.text?.toString().orEmpty()
            val apiKey = etApiKey.text?.toString().orEmpty()
            val effectiveBase = effectiveConnectorBaseInput(baseUrl)
            val effectiveAuth = effectiveAuthBaseInput(authBaseUrl)
            if (effectiveBase.isBlank() || apiKey.isBlank()) {
                Toast.makeText(this, "API key is required. Base URL is required unless this build defines a default.", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val normalized = ApiConfig.normalizeConnectorBaseUrl(effectiveBase)
            if (!isDebugBuild() && normalized.startsWith("http://", ignoreCase = true)) {
                Toast.makeText(
                    this,
                    "Release builds require HTTPS endpoints.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!ApiConfig.isAllowedBaseUrl(effectiveBase)) {
                Toast.makeText(
                    this,
                    "Use HTTPS for production URLs. HTTP is only allowed for local/private development.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!isDebugBuild() && effectiveAuth.isNotBlank() &&
                ApiConfig.normalizeConnectorBaseUrl(effectiveAuth).startsWith("http://", ignoreCase = true)
            ) {
                Toast.makeText(
                    this,
                    "Release builds require HTTPS for the auth URL when set.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!ApiConfig.isAllowedAuthBaseUrl(effectiveAuth)) {
                Toast.makeText(
                    this,
                    "Auth URL must use HTTPS (or be left blank). HTTP is only allowed for local/private development.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!ApiConfig.isValidApiKey(apiKey)) {
                Toast.makeText(
                    this,
                    "API key is invalid. Use your real connector key (not 'change-me').",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            ApiConfig.setConfig(this, baseUrl.trim(), apiKey, authBaseUrl.trim().ifBlank { null })
            val savedAuth = ApiConfig.getAuthBaseUrl(this)
            val authNote = if (savedAuth.isNotBlank()) " · Auth: $savedAuth" else ""
            Toast.makeText(this, "Saved: $normalized$authNote", Toast.LENGTH_LONG).show()
        }

        btnTest.setOnClickListener {
            // Test what you typed (after normalize) — not only last-saved prefs, so Save is not required to try a URL.
            val raw = etBaseUrl.text?.toString().orEmpty()
            val effective = effectiveConnectorBaseInput(raw)
            if (effective.isBlank()) {
                Toast.makeText(this, "Enter a Base URL (or use a build with a default URL)", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val baseUrl = ApiConfig.normalizeConnectorBaseUrl(effective)
            Thread {
                try {
                    val url = URL("$baseUrl/healthz")
                    val conn = url.openConnection() as HttpURLConnection
                    conn.requestMethod = "GET"
                    conn.connectTimeout = 20_000
                    conn.readTimeout = 20_000
                    val code = conn.responseCode

                    val stream = if (code in 200..299) conn.inputStream else conn.errorStream
                    val body = if (stream != null) {
                        BufferedReader(InputStreamReader(stream)).use { it.readText() }
                    } else {
                        ""
                    }
                    runOnUiThread {
                        if (code in 200..299) {
                            Toast.makeText(this, "OK: $baseUrl → HTTP $code $body", Toast.LENGTH_LONG).show()
                        } else {
                            showFailDialog(baseUrl, "HTTP $code", body)
                        }
                    }
                } catch (e: Exception) {
                    runOnUiThread {
                        showFailDialog(baseUrl, e.javaClass.simpleName, e.message ?: "")
                    }
                }
            }.start()
        }

        btnPreflight.setOnClickListener {
            val raw = etBaseUrl.text?.toString().orEmpty()
            val effective = effectiveConnectorBaseInput(raw)
            if (effective.isBlank()) {
                Toast.makeText(this, "Enter a Base URL (or use a build with a default URL)", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val baseUrl = ApiConfig.normalizeConnectorBaseUrl(effective)
            val apiKey = etApiKey.text?.toString().orEmpty().trim()
            if (apiKey.isBlank()) {
                Toast.makeText(this, "Enter API key first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            Thread {
                val checks = mutableListOf<PreflightCheck>()
                checks += checkHealth(baseUrl)
                checks += checkApiRoute(baseUrl, apiKey, "/api/v1/stocks?limit=1&offset=0&search=&refresh=0&mode=stock_take")
                checks += checkApiRoute(baseUrl, apiKey, "/api/v1/test-drives/active")
                checks += checkApiRoute(baseUrl, apiKey, "/api/v1/analytics/forecast")
                runOnUiThread { showPreflightDialog(baseUrl, checks) }
            }.start()
        }

        if (canManageUsers) {
            btnManageUsers.setOnClickListener {
                startActivity(Intent(this, UserManagementActivity::class.java))
            }
        } else {
            btnManageUsers.isEnabled = false
            btnManageUsers.alpha = 0.55f
        }
        if (canViewAudit) {
            btnAuditEvents.setOnClickListener {
                startActivity(Intent(this, AuditEventsActivity::class.java))
            }
        } else {
            btnAuditEvents.isEnabled = false
            btnAuditEvents.alpha = 0.55f
        }

        btnPrivacyPolicy.setOnClickListener {
            openUrl(privacyPolicyUrl)
        }
        btnContactSupport.setOnClickListener {
            composeEmail(
                supportEmail,
                getString(R.string.settings_support_subject)
            )
        }
        btnRequestDeletion.setOnClickListener {
            composeEmail(
                deletionEmail,
                getString(R.string.settings_deletion_subject)
            )
        }
    }

    private fun showFailDialog(triedUrl: String, title: String, detail: String) {
        val msg = if (BuildConfig.LOCK_CONNECTOR_CONFIG) {
            buildString {
                appendLine(detail.ifBlank { "(no detail)" })
                appendLine()
                appendLine("Could not reach the EvolveSA connector (health check failed).")
                appendLine()
                appendLine("Try again on a stable connection, or contact support if this continues.")
            }
        } else {
            buildString {
                appendLine(detail.ifBlank { "(no detail)" })
                appendLine()
                appendLine("Trying: $triedUrl/healthz")
                appendLine()
                appendLine("Checklist:")
                appendLine("1) On PC: cd connector → npm start (see listening on 0.0.0.0:8080)")
                appendLine("2) Tap Save with the URL above, then Test again")
                appendLine("3) Phone + PC on same Wi‑Fi (not guest isolation)")
                appendLine("4) Windows Firewall: allow inbound TCP 8080")
                appendLine("5) Wi‑Fi profile on PC: Private network (not Public)")
                appendLine("6) Or use Cloudflare: cloudflared tunnel --url http://localhost:8080")
            }
        }
        AlertDialog.Builder(this)
            .setTitle("Connection failed: $title")
            .setMessage(msg.trim())
            .setPositiveButton("OK", null)
            .show()
    }

    private fun checkHealth(baseUrl: String): PreflightCheck {
        return try {
            val (code, body) = call("$baseUrl/healthz", null, withTenantHeaders = false)
            if (code in 200..299) {
                PreflightCheck("Connector health", true, "HTTP $code")
            } else {
                PreflightCheck("Connector health", false, "HTTP $code ${body.take(120)}")
            }
        } catch (e: Exception) {
            PreflightCheck("Connector health", false, "${e.javaClass.simpleName}: ${e.message.orEmpty()}")
        }
    }

    private fun checkApiRoute(baseUrl: String, apiKey: String, path: String): PreflightCheck {
        val label = path.substringBefore("?")
        return try {
            val headers = mapOf("Authorization" to "Bearer $apiKey")
            val (code, body) = call("$baseUrl$path", headers, withTenantHeaders = true)
            if (code in 200..299) {
                PreflightCheck(label, true, "HTTP $code")
            } else {
                val shortBody = body.replace('\n', ' ').replace('\r', ' ').take(140)
                val note = when (code) {
                    401 -> "API key rejected"
                    403 -> "Tenant headers/user mapping failed"
                    404 -> "Route missing on backend deploy"
                    else -> shortBody
                }
                PreflightCheck(label, false, "HTTP $code $note")
            }
        } catch (e: Exception) {
            PreflightCheck(label, false, "${e.javaClass.simpleName}: ${e.message.orEmpty()}")
        }
    }

    private fun call(
        urlText: String,
        headers: Map<String, String>?,
        withTenantHeaders: Boolean
    ): Pair<Int, String> {
        val conn = URL(urlText).openConnection() as HttpURLConnection
        conn.requestMethod = "GET"
        conn.connectTimeout = 20_000
        conn.readTimeout = 20_000
        headers?.forEach { (k, v) -> conn.setRequestProperty(k, v) }
        if (withTenantHeaders) {
            AuthStore.getAccessToken(this)?.let { conn.setRequestProperty("X-User-Token", it) }
            AuthStore.getUserId(this)?.let { conn.setRequestProperty("X-User-Id", it) }
            AuthStore.getDisplayName(this)?.let { conn.setRequestProperty("X-User-Name", it) }
            AuthStore.getUserEmail(this)?.let { conn.setRequestProperty("X-User-Email", it) }
            AuthStore.getRole(this)?.let { conn.setRequestProperty("X-User-Role", it) }
            AuthStore.getDealerId(this)?.let { conn.setRequestProperty("X-Dealer-Id", it) }
            AuthStore.getBranchId(this)?.let { conn.setRequestProperty("X-Branch-Id", it) }
        }
        val code = conn.responseCode
        val stream = if (code in 200..299) conn.inputStream else conn.errorStream
        val body = if (stream != null) BufferedReader(InputStreamReader(stream)).use { it.readText() } else ""
        return code to body
    }

    private fun showPreflightDialog(baseUrl: String, checks: List<PreflightCheck>) {
        val passed = checks.count { it.ok }
        val total = checks.size
        val allOk = passed == total
        val summary = buildString {
            appendLine(if (allOk) "Preflight passed ($passed/$total)." else "Preflight found issues ($passed/$total passed).")
            appendLine()
            if (BuildConfig.LOCK_CONNECTOR_CONFIG) {
                appendLine("Connector: managed production endpoint")
            } else {
                appendLine("Base URL: $baseUrl")
            }
            appendLine()
            checks.forEach { c ->
                appendLine("${if (c.ok) "PASS" else "FAIL"} • ${c.name} — ${c.detail}")
            }
            if (!allOk) {
                appendLine()
                appendLine("Quick fix guide:")
                appendLine("• 401: API key mismatch")
                appendLine("• 403: wrong login user/dealer mapping")
                appendLine("• 404: backend route not deployed yet")
            }
        }.trim()
        AlertDialog.Builder(this)
            .setTitle(if (allOk) "Preflight OK" else "Preflight needs attention")
            .setMessage(summary)
            .setPositiveButton("OK", null)
            .show()
    }

    private fun openUrl(url: String) {
        if (isPlaceholder(url)) {
            Toast.makeText(this, getString(R.string.settings_contact_not_configured), Toast.LENGTH_LONG).show()
            return
        }
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
        if (intent.resolveActivity(packageManager) != null) {
            startActivity(intent)
        } else {
            Toast.makeText(this, "No browser app found", Toast.LENGTH_LONG).show()
        }
    }

    private fun composeEmail(to: String, subject: String) {
        if (isPlaceholder(to)) {
            Toast.makeText(this, getString(R.string.settings_contact_not_configured), Toast.LENGTH_LONG).show()
            return
        }
        val intent = Intent(Intent.ACTION_SENDTO).apply {
            data = Uri.parse("mailto:$to")
            putExtra(Intent.EXTRA_SUBJECT, subject)
        }
        if (intent.resolveActivity(packageManager) != null) {
            startActivity(intent)
        } else {
            Toast.makeText(this, "No email app found", Toast.LENGTH_LONG).show()
        }
    }

    /** Typed value or build-time default from strings.xml (EvolveSA flavor sets production URLs). */
    private fun effectiveConnectorBaseInput(typed: String): String {
        val t = typed.trim()
        if (t.isNotBlank()) return t
        return getString(R.string.default_connector_base_url).trim()
    }

    private fun effectiveAuthBaseInput(typed: String): String {
        val t = typed.trim()
        if (t.isNotBlank()) return t
        return getString(R.string.default_auth_base_url).trim()
    }

    private fun isPlaceholder(value: String): Boolean {
        val v = value.trim().lowercase()
        return v.isBlank() || v.contains("example.com")
    }

    private fun isDebugBuild(): Boolean {
        return (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
    }
}

