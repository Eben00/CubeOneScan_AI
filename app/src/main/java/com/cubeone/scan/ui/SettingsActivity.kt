package com.cubeone.scan.ui

import android.content.Intent
import android.content.pm.ApplicationInfo
import android.net.Uri
import android.os.Bundle
import android.widget.Button
import android.widget.EditText
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
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
        val canAccessSettings = isOfflineDemo || role == "dealer_principal" || role == "sales_manager"
        if (!canAccessSettings) {
            Toast.makeText(this, "You are not allowed to access settings", Toast.LENGTH_LONG).show()
            finish()
            return
        }
        setContentView(R.layout.activity_settings)

        val etBaseUrl = findViewById<EditText>(R.id.etBaseUrl)
        val etAuthBaseUrl = findViewById<EditText>(R.id.etAuthBaseUrl)
        val etApiKey = findViewById<EditText>(R.id.etApiKey)
        val btnSave = findViewById<Button>(R.id.btnSave)
        val btnTest = findViewById<Button>(R.id.btnTest)
        val btnManageUsers = findViewById<Button>(R.id.btnManageUsers)
        val btnAuditEvents = findViewById<Button>(R.id.btnAuditEvents)
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
            if (baseUrl.isBlank() || apiKey.isBlank()) {
                Toast.makeText(this, "Base URL and API key are required", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val normalized = ApiConfig.normalizeConnectorBaseUrl(baseUrl)
            if (!isDebugBuild() && normalized.startsWith("http://", ignoreCase = true)) {
                Toast.makeText(
                    this,
                    "Release builds require HTTPS endpoints.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!ApiConfig.isAllowedBaseUrl(baseUrl)) {
                Toast.makeText(
                    this,
                    "Use HTTPS for production URLs. HTTP is only allowed for local/private development.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!isDebugBuild() && authBaseUrl.isNotBlank() &&
                ApiConfig.normalizeConnectorBaseUrl(authBaseUrl).startsWith("http://", ignoreCase = true)
            ) {
                Toast.makeText(
                    this,
                    "Release builds require HTTPS for the auth URL when set.",
                    Toast.LENGTH_LONG
                ).show()
                return@setOnClickListener
            }
            if (!ApiConfig.isAllowedAuthBaseUrl(authBaseUrl)) {
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
            ApiConfig.setConfig(this, baseUrl, apiKey, authBaseUrl.ifBlank { null })
            val savedAuth = ApiConfig.getAuthBaseUrl(this)
            val authNote = if (savedAuth.isNotBlank()) " · Auth: $savedAuth" else ""
            Toast.makeText(this, "Saved: $normalized$authNote", Toast.LENGTH_LONG).show()
        }

        btnTest.setOnClickListener {
            // Test what you typed (after normalize) — not only last-saved prefs, so Save is not required to try a URL.
            val raw = etBaseUrl.text?.toString().orEmpty()
            if (raw.isBlank()) {
                Toast.makeText(this, "Enter a Base URL first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val baseUrl = ApiConfig.normalizeConnectorBaseUrl(raw)
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

        btnManageUsers.setOnClickListener {
            startActivity(Intent(this, UserManagementActivity::class.java))
        }
        btnAuditEvents.setOnClickListener {
            startActivity(Intent(this, AuditEventsActivity::class.java))
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
        val msg = buildString {
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
        AlertDialog.Builder(this)
            .setTitle("Connection failed: $title")
            .setMessage(msg.trim())
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

    private fun isPlaceholder(value: String): Boolean {
        val v = value.trim().lowercase()
        return v.isBlank() || v.contains("example.com")
    }

    private fun isDebugBuild(): Boolean {
        return (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
    }
}

