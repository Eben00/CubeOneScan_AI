package com.cubeone.scan.ui

import android.content.Intent
import android.os.Bundle
import android.widget.Toast
import android.widget.TextView
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AdminActionResult
import com.cubeone.scan.core.auth.AuthApiService
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.services.CommandApiService
import com.cubeone.scan.scanner.ScannerActivity
import com.google.android.material.button.MaterialButton
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

class HomeActivity : AppCompatActivity() {
    private var refreshQueueBadgeFn: (() -> Unit)? = null

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
        if (AuthStore.getAccessToken(this).isNullOrBlank()) {
            Toast.makeText(this, "Auth token missing (redirecting to login)", Toast.LENGTH_LONG).show()
            startActivity(Intent(this, LoginActivity::class.java))
            finish()
            return
        }
        if (AuthStore.isMustChangePassword(this)) {
            startActivity(Intent(this, ChangePasswordActivity::class.java))
            finish()
            return
        }
        setContentView(R.layout.activity_home)
        val role = normalizeBusinessRole(AuthStore.getRole(this).orEmpty())
        val token = AuthStore.getAccessToken(this).orEmpty()
        val isOfflineDemo = token == "offline-demo-token"
        val canAccessSettings = isOfflineDemo || role == "dealer_principal" || role == "sales_manager" || role == "sales_person"
        val canAccessApprovals = role == "dealer_principal" || role == "sales_manager"
        val canAccessAudit = role == "dealer_principal" || role == "sales_manager" || role == "sales_person"
        val canTestDriveWorkflow = role == "dealer_principal" || role == "sales_manager" || role == "sales_person"
        val canAccessOpsDashboard = role == "dealer_principal" || role == "sales_manager" || role == "sales_person"

        fun openScanner(mode: String, postAction: String = ScannerActivity.ACTION_NONE) {
            val intent = Intent(this, ScannerActivity::class.java).apply {
                putExtra(ScannerActivity.EXTRA_SCAN_MODE, mode)
                putExtra(ScannerActivity.EXTRA_POST_SCAN_ACTION, postAction)
            }
            startActivity(intent)
        }

        findViewById<MaterialButton>(R.id.btnScanDriverLicense).setOnClickListener {
            openScanner(ScannerActivity.MODE_DRIVER)
        }
        findViewById<MaterialButton>(R.id.btnScanVehicleLicense).setOnClickListener {
            openScanner(ScannerActivity.MODE_VEHICLE)
        }
        findViewById<MaterialButton>(R.id.btnStockTakeWorkflow).setOnClickListener {
            openScanner(ScannerActivity.MODE_VEHICLE, ScannerActivity.ACTION_STOCK_TAKE)
        }
        findViewById<MaterialButton>(R.id.btnTradeInWorkflow).setOnClickListener {
            openScanner(ScannerActivity.MODE_VEHICLE, ScannerActivity.ACTION_TRADE_IN)
        }
        val btnTestDriveWorkflow = findViewById<MaterialButton>(R.id.btnTestDriveWorkflow)
        if (canTestDriveWorkflow) {
            btnTestDriveWorkflow.visibility = android.view.View.VISIBLE
            btnTestDriveWorkflow.setOnClickListener {
                openScanner(ScannerActivity.MODE_DRIVER, ScannerActivity.ACTION_TEST_DRIVE)
            }
        } else {
            btnTestDriveWorkflow.visibility = android.view.View.GONE
        }
        val tvQueueBadge = findViewById<TextView>(R.id.tvQueueBadge)
        val tvCommandHealth = findViewById<TextView>(R.id.tvCommandHealth)
        val btnRetryQueue = findViewById<MaterialButton>(R.id.btnRetryQueue)
        fun refreshQueueBadge() {
            val health = CommandApiService.getCommandHealthSnapshot(this)
            val count = health.queuedCount
            tvQueueBadge.text = "Offline queue: $count pending"
            val updated = if (health.updatedAtMs > 0L) {
                SimpleDateFormat("HH:mm", Locale.getDefault()).format(Date(health.updatedAtMs))
            } else {
                "now"
            }
            val healthText = if (!health.lastError.isNullOrBlank()) {
                "Sync health: attention needed ($updated) • ${health.lastError}"
            } else {
                "Sync health: stable • last flush sent ${health.lastFlushSent}, remaining ${health.lastFlushRemaining}"
            }
            tvCommandHealth.text = healthText
        }
        refreshQueueBadgeFn = ::refreshQueueBadge
        refreshQueueBadge()
        btnRetryQueue.setOnClickListener {
            btnRetryQueue.isEnabled = false
            CommandApiService.flushQueuedCommands(
                context = this,
                onProgress = { remaining ->
                    runOnUiThread { tvQueueBadge.text = "Offline queue: $remaining pending" }
                },
                onDone = { sent, remaining ->
                    runOnUiThread {
                        btnRetryQueue.isEnabled = true
                        tvQueueBadge.text = "Offline queue: $remaining pending"
                        Toast.makeText(this, "Retry complete. Sent $sent, remaining $remaining", Toast.LENGTH_LONG).show()
                    }
                }
            )
        }
        val btnSettings = findViewById<MaterialButton>(R.id.btnSettings)
        if (canAccessSettings) {
            btnSettings.visibility = android.view.View.VISIBLE
            btnSettings.setOnClickListener {
                startActivity(Intent(this, SettingsActivity::class.java))
            }
        } else {
            btnSettings.visibility = android.view.View.GONE
        }
        val btnApprovals = findViewById<MaterialButton>(R.id.btnApprovals)
        if (canAccessApprovals) {
            btnApprovals.visibility = android.view.View.VISIBLE
            btnApprovals.setOnClickListener {
                startActivity(Intent(this, ApprovalsActivity::class.java))
            }
        } else {
            btnApprovals.visibility = android.view.View.GONE
        }
        val btnAuditEvents = findViewById<MaterialButton>(R.id.btnAuditEvents)
        if (canAccessAudit) {
            btnAuditEvents.visibility = android.view.View.VISIBLE
            btnAuditEvents.setOnClickListener {
                startActivity(Intent(this, AuditEventsActivity::class.java))
            }
        } else {
            btnAuditEvents.visibility = android.view.View.GONE
        }
        val btnOperationsDashboard = findViewById<MaterialButton>(R.id.btnOperationsDashboard)
        if (canAccessOpsDashboard) {
            btnOperationsDashboard.visibility = android.view.View.VISIBLE
            btnOperationsDashboard.setOnClickListener {
                startActivity(Intent(this, OperationsDashboardActivity::class.java))
            }
        } else {
            btnOperationsDashboard.visibility = android.view.View.GONE
        }
        val btnLogoutAll = findViewById<MaterialButton>(R.id.btnLogoutAll)
        btnLogoutAll.setOnClickListener {
            AlertDialog.Builder(this)
                .setTitle("Log out all devices")
                .setMessage("This will revoke all active sessions for your account. Continue?")
                .setNegativeButton("Cancel", null)
                .setPositiveButton("Log out all") { _, _ ->
                    Thread {
                        val result = AuthApiService.logoutAllSessions(this)
                        runOnUiThread {
                            when (result) {
                                AdminActionResult.Success -> {
                                    AuthStore.clear(this)
                                    Toast.makeText(this, "All sessions revoked", Toast.LENGTH_LONG).show()
                                    startActivity(Intent(this, LoginActivity::class.java))
                                    finish()
                                }
                                is AdminActionResult.Error -> {
                                    Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                                }
                            }
                        }
                    }.start()
                }
                .show()
        }
        findViewById<MaterialButton>(R.id.btnLogout).setOnClickListener {
            Thread {
                AuthApiService.logoutCurrentSession(this)
                runOnUiThread {
                    AuthStore.clear(this)
                    startActivity(Intent(this, LoginActivity::class.java))
                    finish()
                }
            }.start()
        }
    }

    override fun onResume() {
        super.onResume()
        refreshQueueBadgeFn?.invoke()
    }
}
