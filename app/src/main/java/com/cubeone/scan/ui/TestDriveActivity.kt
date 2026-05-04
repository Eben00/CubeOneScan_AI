package com.cubeone.scan.ui

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuthStore
import com.cubeone.scan.scanner.ScannerActivity
import com.cubeone.scan.services.CommandApiService
import com.cubeone.scan.utils.WorkflowState
import com.google.android.material.button.MaterialButton
import org.json.JSONObject
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale
import java.util.TimeZone

class TestDriveActivity : AppCompatActivity() {
    private var activeTestDriveSessionId: String? = null

    private val scanTestDriveVehicleLauncher =
        registerForActivityResult(ActivityResultContracts.StartActivityForResult()) { result ->
            if (result.resultCode != RESULT_OK) return@registerForActivityResult
            val data = result.data ?: return@registerForActivityResult
            val registration = data.getStringExtra("REGISTRATION").orEmpty().trim()
            val licenceNumber = data.getStringExtra("LICENCE_NUMBER").orEmpty().trim()
            val make = data.getStringExtra("MAKE").orEmpty().trim()
            val model = data.getStringExtra("MODEL").orEmpty().trim()
            val vehicleRef = registration.ifBlank { licenceNumber }
            if (vehicleRef.isBlank()) {
                Toast.makeText(this, "Vehicle scan completed but reference was empty", Toast.LENGTH_LONG).show()
                return@registerForActivityResult
            }
            findViewById<EditText>(R.id.etTestDriveVehicleRef).setText(vehicleRef)
            val summary = listOf(make, model).filter { it.isNotBlank() }.joinToString(" ")
            val msg = if (summary.isNotBlank()) "Vehicle captured: $summary ($vehicleRef)" else "Vehicle captured: $vehicleRef"
            Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
        }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_test_drive)

        val surname = intent.getStringExtra("SURNAME").orEmpty().trim()
        val initials = intent.getStringExtra("NAMES").orEmpty().trim()
        val idNumber = intent.getStringExtra("ID_NUMBER").orEmpty().trim()
        val licenseNumber = intent.getStringExtra("LICENSE_NUMBER").orEmpty().trim()

        findViewById<TextView>(R.id.tvDriverSummary).text =
            listOf(
                listOf(initials, surname).filter { it.isNotBlank() }.joinToString(" ").ifBlank { "Driver" },
                if (idNumber.isBlank()) null else "ID $idNumber",
                if (licenseNumber.isBlank()) null else "Licence $licenseNumber"
            ).filterNotNull().joinToString(" • ")

        findViewById<MaterialButton>(R.id.btnScanTestDriveVehicle).setOnClickListener {
            val intent = Intent(this, ScannerActivity::class.java).apply {
                putExtra(ScannerActivity.EXTRA_SCAN_MODE, ScannerActivity.MODE_VEHICLE)
                putExtra(ScannerActivity.EXTRA_POST_SCAN_ACTION, ScannerActivity.ACTION_TEST_DRIVE)
            }
            scanTestDriveVehicleLauncher.launch(intent)
        }

        findViewById<MaterialButton>(R.id.btnCreateLeadForTestDrive).setOnClickListener {
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim().replace(" ", "")
            if (cellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val payload = JSONObject().apply {
                put(
                    "driverLicense",
                    JSONObject().apply {
                        put("firstName", initials)
                        put("surname", surname)
                        put("idNumber", idNumber)
                        put("licenseNumber", licenseNumber)
                        put("phone", cellphone)
                    }
                )
                put("phone", cellphone)
                put(
                    "createdBy",
                    JSONObject().apply {
                        put("userId", AuthStore.getUserId(this@TestDriveActivity).orEmpty())
                        put("name", AuthStore.getDisplayName(this@TestDriveActivity).orEmpty())
                        put("email", AuthStore.getUserEmail(this@TestDriveActivity).orEmpty())
                        put("role", AuthStore.getRole(this@TestDriveActivity).orEmpty())
                    }
                )
            }
            CommandApiService.createCommand(
                context = this,
                commandType = "CREATE_LEAD",
                payload = payload,
                onSuccess = { resp ->
                    WorkflowState.setLeadCorrelationId(this, resp.correlationId)
                    runOnUiThread { Toast.makeText(this, "Lead queued: ${resp.correlationId}", Toast.LENGTH_LONG).show() }
                    watchLeadCommandCompletion(resp.correlationId)
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Create lead failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnStartTestDrive).setOnClickListener {
            val leadCorrelationId = WorkflowState.getLeadCorrelationId(this)
            if (leadCorrelationId.isNullOrBlank()) {
                Toast.makeText(this, "Create lead first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val vehicleRef = findViewById<EditText>(R.id.etTestDriveVehicleRef).text?.toString().orEmpty().trim()
            if (vehicleRef.isBlank()) {
                Toast.makeText(this, "Enter vehicle ref", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val cellphone = findViewById<EditText>(R.id.etLeadCellphone).text?.toString().orEmpty().trim().replace(" ", "")
            if (cellphone.length < 10) {
                Toast.makeText(this, getString(R.string.lead_cellphone_required), Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            val emergency = findViewById<EditText>(R.id.etTestDriveEmergencyMobile).text?.toString().orEmpty().trim()
            val returnMinutes = findViewById<EditText>(R.id.etPlannedReturnMinutes).text?.toString().orEmpty().trim().toIntOrNull() ?: 30
            val cal = Calendar.getInstance()
            cal.add(Calendar.MINUTE, returnMinutes.coerceIn(5, 180))
            val payload = JSONObject().apply {
                put("leadId", leadCorrelationId)
                put("vehicleRef", vehicleRef)
                put("driverIdNumber", idNumber)
                put("mobile", cellphone)
                put("emergencyMobile", emergency)
                put(
                    "plannedReturnAt",
                    SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US).apply {
                        timeZone = TimeZone.getTimeZone("UTC")
                    }.format(cal.time)
                )
            }
            CommandApiService.startTestDrive(
                context = this,
                payload = payload,
                onSuccess = { json ->
                    val session = json.optJSONObject("session")
                    activeTestDriveSessionId = session?.optString("sessionId").orEmpty().ifBlank { null }
                    runOnUiThread {
                        findViewById<TextView>(R.id.tvTestDriveStatus).text = "Active session: ${activeTestDriveSessionId ?: "-"}"
                        Toast.makeText(this, "Test drive started", Toast.LENGTH_LONG).show()
                    }
                    refreshActiveTestDriveStatus()
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Start test drive failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnTestDriveCheckin).setOnClickListener {
            val sessionId = activeTestDriveSessionId
            if (sessionId.isNullOrBlank()) {
                Toast.makeText(this, "Start a test drive first", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            CommandApiService.testDriveCheckin(
                context = this,
                sessionId = sessionId,
                payload = JSONObject().apply { put("note", "Salesman safety check-in") },
                onSuccess = {
                    runOnUiThread { Toast.makeText(this, "Check-in captured", Toast.LENGTH_SHORT).show() }
                    refreshActiveTestDriveStatus()
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Check-in failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnCompleteTestDrive).setOnClickListener {
            val sessionId = activeTestDriveSessionId
            if (sessionId.isNullOrBlank()) {
                Toast.makeText(this, "No active test drive", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            CommandApiService.completeTestDrive(
                context = this,
                sessionId = sessionId,
                payload = JSONObject().apply { put("returnNotes", "Vehicle returned safely") },
                onSuccess = {
                    activeTestDriveSessionId = null
                    runOnUiThread {
                        findViewById<TextView>(R.id.tvTestDriveStatus).text = getString(R.string.test_drive_status_placeholder)
                        Toast.makeText(this, "Test drive completed", Toast.LENGTH_SHORT).show()
                    }
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Complete failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        refreshActiveTestDriveStatus()
    }

    private fun watchLeadCommandCompletion(correlationId: String) {
        Thread {
            repeat(8) {
                try {
                    Thread.sleep(1500)
                    val status = CommandApiService.getCommandStatusBlocking(this, correlationId)
                    when (status.status.lowercase()) {
                        "done" -> {
                            runOnUiThread {
                                val leadId = status.result?.optString("leadId").orEmpty()
                                val msg = if (leadId.isNotBlank()) "Lead synced (ID: $leadId)" else "Lead synced"
                                Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                            }
                            return@Thread
                        }
                        "failed" -> {
                            runOnUiThread {
                                Toast.makeText(this, "Lead failed: ${status.error ?: "Unknown error"}", Toast.LENGTH_LONG).show()
                            }
                            return@Thread
                        }
                    }
                } catch (_: Exception) {
                }
            }
        }.start()
    }

    private fun refreshActiveTestDriveStatus() {
        CommandApiService.getActiveTestDrives(
            context = this,
            onSuccess = { json ->
                val arr = json.optJSONArray("sessions")
                var statusText = getString(R.string.test_drive_status_placeholder)
                if (arr != null && arr.length() > 0) {
                    val first = arr.optJSONObject(0)
                    val sid = first?.optString("sessionId").orEmpty()
                    val safety = first?.optString("safetyStatus").orEmpty().ifBlank { "on_track" }
                    val overdue = first?.optInt("overdueMinutes", 0) ?: 0
                    if (sid.isNotBlank()) activeTestDriveSessionId = sid
                    statusText = if (overdue > 0) "Safety alert: $safety ($overdue min overdue)" else "Active test drive: $safety"
                }
                runOnUiThread {
                    val view = findViewById<TextView>(R.id.tvTestDriveStatus)
                    view.text = statusText
                    val danger = statusText.contains("overdue", ignoreCase = true)
                    view.setTextColor(if (danger) Color.parseColor("#B3261E") else ContextCompat.getColor(this, R.color.text_secondary))
                }
            },
            onError = { err ->
                runOnUiThread {
                    findViewById<TextView>(R.id.tvTestDriveStatus).text = "Test drive status unavailable: $err"
                }
            }
        )
    }
}
