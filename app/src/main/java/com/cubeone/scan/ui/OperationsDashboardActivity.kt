package com.cubeone.scan.ui

import android.os.Bundle
import android.widget.EditText
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.services.CommandApiService
import com.google.android.material.button.MaterialButton
import org.json.JSONObject

class OperationsDashboardActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_operations_dashboard)

        findViewById<MaterialButton>(R.id.btnSaveTargets).setOnClickListener {
            val leads = findViewById<EditText>(R.id.etTargetLeads).text?.toString().orEmpty().trim().toIntOrNull() ?: 0
            val testDrives = findViewById<EditText>(R.id.etTargetTestDrives).text?.toString().orEmpty().trim().toIntOrNull() ?: 0
            val body = JSONObject().apply {
                put(
                    "targets",
                    JSONObject().apply {
                        put("leads", leads)
                        put("testDrives", testDrives)
                    }
                )
            }
            CommandApiService.saveTargets(
                context = this,
                targets = body,
                onSuccess = {
                    runOnUiThread { Toast.makeText(this, "Targets saved", Toast.LENGTH_SHORT).show() }
                },
                onError = { err ->
                    runOnUiThread { Toast.makeText(this, "Save targets failed: $err", Toast.LENGTH_LONG).show() }
                }
            )
        }

        findViewById<MaterialButton>(R.id.btnRefreshOps).setOnClickListener {
            refreshOpsSnapshot()
        }
        refreshOpsSnapshot()
    }

    private fun refreshOpsSnapshot() {
        CommandApiService.getForecast(
            context = this,
            onSuccess = { json ->
                val forecast = json.optJSONObject("forecast")
                val alerts = json.optJSONArray("alerts")
                val leadsFc = forecast?.opt("leads")?.toString().orEmpty().ifBlank { "-" }
                val sharesFc = forecast?.opt("shares")?.toString().orEmpty().ifBlank { "-" }
                val alertBand = alerts?.optJSONObject(0)?.optString("band").orEmpty().ifBlank { "unknown" }
                runOnUiThread {
                    findViewById<TextView>(R.id.tvOpsForecast).text =
                        "Forecast this month -> Leads: $leadsFc, Shares: $sharesFc, Alert: $alertBand"
                }
            },
            onError = { err ->
                runOnUiThread { findViewById<TextView>(R.id.tvOpsForecast).text = "Forecast unavailable: $err" }
            }
        )
        CommandApiService.getOemRollup(
            context = this,
            onSuccess = { json ->
                val dealers = json.optJSONArray("dealers")
                val total = dealers?.length() ?: 0
                runOnUiThread {
                    findViewById<TextView>(R.id.tvOpsOemRollup).text = "OEM rollup loaded for $total dealer(s)"
                }
            },
            onError = { err ->
                runOnUiThread { findViewById<TextView>(R.id.tvOpsOemRollup).text = "OEM rollup unavailable: $err" }
            }
        )
    }
}
