package com.cubeone.scan.ui

import android.content.Intent
import android.net.Uri
import android.os.Bundle
import android.view.View
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import com.cubeone.scan.R
import com.cubeone.scan.services.CommandApiService
import com.google.android.material.button.MaterialButton
import org.json.JSONObject
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.TimeZone

class ApprovalsActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_approvals)

        findViewById<MaterialButton>(R.id.btnRefreshApprovals).setOnClickListener { loadApprovals() }
        loadApprovals()
    }

    private fun loadApprovals() {
        val container = findViewById<LinearLayout>(R.id.listApprovals)
        val empty = findViewById<TextView>(R.id.tvNoApprovals)
        container.removeAllViews()
        empty.visibility = View.GONE

        CommandApiService.getPendingApprovals(
            context = this,
            onSuccess = { items ->
                runOnUiThread {
                    if (items.isEmpty()) {
                        empty.visibility = View.VISIBLE
                        return@runOnUiThread
                    }
                    items.forEach { item ->
                        val row = layoutInflater.inflate(R.layout.item_approval, container, false)
                        row.setOnClickListener { showApprovalDetails(item) }
                        row.findViewById<TextView>(R.id.tvApprovalTitle).text =
                            "${item.requestedCommandType ?: item.commandType} • ${item.correlationId.take(14)}…"
                        bindRiskBadge(row.findViewById(R.id.tvApprovalRisk), item.payload)
                        val seenText = if (item.managerSeen) {
                            "Seen: ✅${if (!item.managerSeenAt.isNullOrBlank()) " ${formatTimestamp(item.managerSeenAt)}" else ""}"
                        } else {
                            "Seen: ❌ Not seen by manager yet"
                        }
                        row.findViewById<TextView>(R.id.tvApprovalMeta).text =
                            "Status: ${item.status}  |  Created: ${formatTimestamp(item.createdAt)}\n$seenText\n${summarizePayload(item.payload)}"

                        row.findViewById<MaterialButton>(R.id.btnDetails).setOnClickListener {
                            showApprovalDetails(item)
                        }

                        row.findViewById<MaterialButton>(R.id.btnApprove).setOnClickListener {
                            CommandApiService.approveRequest(
                                context = this,
                                correlationId = item.correlationId,
                                onSuccess = {
                                    runOnUiThread {
                                        Toast.makeText(this, "Approved", Toast.LENGTH_SHORT).show()
                                        loadApprovals()
                                    }
                                },
                                onError = { err ->
                                    runOnUiThread { Toast.makeText(this, err, Toast.LENGTH_LONG).show() }
                                }
                            )
                        }
                        row.findViewById<MaterialButton>(R.id.btnReject).setOnClickListener {
                            AlertDialog.Builder(this)
                                .setTitle("Reject request")
                                .setMessage("Reject ${item.correlationId}?")
                                .setPositiveButton("Reject") { _, _ ->
                                    CommandApiService.rejectRequest(
                                        context = this,
                                        correlationId = item.correlationId,
                                        reason = "Rejected by manager",
                                        onSuccess = {
                                            runOnUiThread {
                                                Toast.makeText(this, "Rejected", Toast.LENGTH_SHORT).show()
                                                loadApprovals()
                                            }
                                        },
                                        onError = { err ->
                                            runOnUiThread { Toast.makeText(this, err, Toast.LENGTH_LONG).show() }
                                        }
                                    )
                                }
                                .setNegativeButton("Cancel", null)
                                .show()
                        }
                        container.addView(row)
                    }
                }
            },
            onError = { err ->
                runOnUiThread { Toast.makeText(this, err, Toast.LENGTH_LONG).show() }
            }
        )
    }

    private fun summarizePayload(payload: JSONObject?): String {
        if (payload == null) return "No details in payload"
        val damages = payload.optJSONArray("damages")?.length() ?: 0
        val photos = payload.optJSONObject("photos")
        val photoCount = listOf("front", "left", "right", "back")
            .count { !photos?.optString(it).isNullOrBlank() }
        val reconTotal = computeReconTotal(payload)
        val valuation = buildString {
            val trade = payload.optString("tradePrice")
            val retail = payload.optString("retailPrice")
            val market = payload.optString("marketPrice")
            if (trade.isNotBlank() || retail.isNotBlank() || market.isNotBlank()) {
                append("Values: Trade=${if (trade.isBlank()) "—" else trade}, Retail=${if (retail.isBlank()) "—" else retail}, Market=${if (market.isBlank()) "—" else market}  |  ")
            }
        }
        return valuation + "Damages: $damages  |  Photos: $photoCount  |  Recon total: R %,d".format(reconTotal)
    }

    private fun formatTimestamp(raw: String?): String {
        val text = raw?.trim().orEmpty()
        if (text.isBlank()) return "—"
        return try {
            val input = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX", Locale.US).apply {
                timeZone = TimeZone.getTimeZone("UTC")
            }
            val output = SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.getDefault())
            val parsed = input.parse(text)
            if (parsed == null) text else output.format(parsed)
        } catch (_: Exception) {
            text
        }
    }

    private fun computeReconTotal(payload: JSONObject?): Long {
        val arr = payload?.optJSONArray("damages") ?: return 0L
        var total = 0L
        for (i in 0 until arr.length()) {
            total += arr.optJSONObject(i)?.optLong("reconCost", 0L) ?: 0L
        }
        return total
    }

    private fun bindRiskBadge(view: TextView, payload: JSONObject?) {
        val total = computeReconTotal(payload)
        val (label, bgColor, textColor) = when {
            total >= 15000L -> Triple(
                "HIGH RECON • R %,d".format(total),
                ContextCompat.getColor(this, R.color.primary_dark),
                ContextCompat.getColor(this, R.color.white)
            )
            total >= 5000L -> Triple(
                "MEDIUM RECON • R %,d".format(total),
                ContextCompat.getColor(this, R.color.trade_amber),
                ContextCompat.getColor(this, R.color.text_primary)
            )
            total > 0L -> Triple(
                "LOW RECON • R %,d".format(total),
                ContextCompat.getColor(this, R.color.retail_green),
                ContextCompat.getColor(this, R.color.white)
            )
            else -> Triple(
                "NO RECON COST CAPTURED",
                ContextCompat.getColor(this, R.color.surface_variant),
                ContextCompat.getColor(this, R.color.text_secondary)
            )
        }
        view.visibility = View.VISIBLE
        view.text = label
        view.setBackgroundColor(bgColor)
        view.setTextColor(textColor)
    }

    private fun showApprovalDetails(item: CommandApiService.ApprovalItem) {
        val payload = item.payload ?: JSONObject()
        val wrapper = ScrollView(this)
        val container = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding(36, 20, 36, 20)
        }
        wrapper.addView(container)

        fun addLine(label: String, value: String) {
            val v = value.trim()
            if (v.isBlank()) return
            container.addView(TextView(this).apply {
                text = "$label: $v"
                textSize = 14f
            })
        }

        addLine("Correlation", item.correlationId)
        addLine("Type", item.requestedCommandType ?: item.commandType)
        addLine("Status", item.status)
        addLine("Created", formatTimestamp(item.createdAt))
        addLine("VIN", payload.optString("vin"))
        addLine("Mileage", payload.optString("mileage"))
        addLine("Condition", payload.optString("condition"))
        addLine("Currency", payload.optString("currency"))
        addLine("Trade Value", payload.optString("tradePrice"))
        addLine("Retail Value", payload.optString("retailPrice"))
        addLine("Market Value", payload.optString("marketPrice"))

        val photos = payload.optJSONObject("photos") ?: JSONObject()
        val photoKeys = listOf("front", "left", "right", "back")
        val hasPhoto = photoKeys.any { photos.optString(it).isNotBlank() }
        if (hasPhoto) {
            container.addView(TextView(this).apply {
                text = "Photos"
                textSize = 16f
            })
            for (key in photoKeys) {
                val url = photos.optString(key).trim()
                if (url.isBlank()) continue
                container.addView(MaterialButton(this).apply {
                    text = "Open ${key.replaceFirstChar { it.uppercase() }} photo"
                    setOnClickListener {
                        try {
                            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
                        } catch (e: Exception) {
                            Toast.makeText(this@ApprovalsActivity, "Unable to open photo URL", Toast.LENGTH_SHORT).show()
                        }
                    }
                })
            }
        }

        val damages = payload.optJSONArray("damages")
        if (damages != null && damages.length() > 0) {
            container.addView(TextView(this).apply {
                text = "Damages"
                textSize = 16f
            })
            var total = 0L
            for (i in 0 until damages.length()) {
                val d = damages.optJSONObject(i) ?: continue
                val zone = d.optString("zone")
                val severity = d.optString("severity")
                val desc = d.optString("description")
                val recon = d.optLong("reconCost", 0L)
                total += recon
                container.addView(TextView(this).apply {
                    text = "• ${if (zone.isBlank()) "Unknown zone" else zone} | ${if (severity.isBlank()) "n/a" else severity} | R %,d\n  ${if (desc.isBlank()) "(no description)" else desc}".format(recon)
                    textSize = 13f
                })
            }
            container.addView(TextView(this).apply {
                text = "Total recon estimate: R %,d".format(total)
                textSize = 14f
            })
        }

        AlertDialog.Builder(this)
            .setTitle("Approval details")
            .setView(wrapper)
            .setPositiveButton("Close", null)
            .show()
    }
}
