package com.cubeone.scan.scanner

import android.os.Bundle
import android.widget.Button
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R

class LicenseResultActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_scan_result)

        if (!intent.getBooleanExtra("SUCCESS", false)) {
            showError(intent.getStringExtra("ERROR_MSG") ?: "Failed to decode")
            return
        }

        // Extract all extras safely
        val surname = intent.getStringExtra("SURNAME") ?: "N/A"
        val initials = intent.getStringExtra("INITIALS") ?: "N/A"
        val idNumber = intent.getStringExtra("ID_NUMBER") ?: "N/A"
        val licenseNumber = intent.getStringExtra("LICENSE_NUMBER") ?: "N/A"
        val validFrom = intent.getStringExtra("VALID_FROM") ?: "N/A"
        val validTo = intent.getStringExtra("VALID_TO") ?: "N/A"
        val vehicleCodes = intent.getStringExtra("VEHICLE_CODES") ?: "N/A"

        // Bind to views
        findViewById<TextView>(R.id.tvIdNumber)?.text = "ID Number: $idNumber"
        findViewById<TextView>(R.id.tvSurname)?.text = "Surname: $surname"
        findViewById<TextView>(R.id.tvInitials)?.text = "Initials: $initials"
        findViewById<TextView>(R.id.tvLicense)?.text = "License No: $licenseNumber"
        findViewById<TextView>(R.id.tvValidFrom)?.text = "Valid From: $validFrom"
        findViewById<TextView>(R.id.tvValidTo)?.text = "Valid To: $validTo"
        findViewById<TextView>(R.id.tvVehicleCodes)?.text = "Vehicle Codes: $vehicleCodes"

        findViewById<Button>(R.id.btnDone)?.setOnClickListener {
            finish()
        }
    }

    private fun showError(message: String) {
        Toast.makeText(this, message, Toast.LENGTH_LONG).show()
        findViewById<TextView>(R.id.tvIdNumber)?.text = "Error: $message"
        findViewById<Button>(R.id.btnDone)?.setOnClickListener { finish() }
    }
}