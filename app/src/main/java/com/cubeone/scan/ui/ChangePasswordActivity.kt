package com.cubeone.scan.ui

import android.content.Intent
import android.os.Bundle
import android.view.View
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AdminActionResult
import com.cubeone.scan.core.auth.AuthApiService
import com.cubeone.scan.core.auth.AuthStore
import com.google.android.material.button.MaterialButton
import com.google.android.material.progressindicator.CircularProgressIndicator
import com.google.android.material.textfield.TextInputEditText

class ChangePasswordActivity : AppCompatActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_change_password)

        val etCurrent = findViewById<TextInputEditText>(R.id.etCurrentPassword)
        val etNew = findViewById<TextInputEditText>(R.id.etNewPassword)
        val etConfirm = findViewById<TextInputEditText>(R.id.etConfirmPassword)
        val btnSubmit = findViewById<MaterialButton>(R.id.btnSubmitPasswordChange)
        val btnLogout = findViewById<MaterialButton>(R.id.btnLogoutPasswordChange)
        val progress = findViewById<CircularProgressIndicator>(R.id.progressChangePassword)

        btnLogout.setOnClickListener {
            AuthStore.clear(this)
            startActivity(Intent(this, LoginActivity::class.java))
            finish()
        }

        btnSubmit.setOnClickListener {
            val currentPassword = etCurrent.text?.toString().orEmpty()
            val newPassword = etNew.text?.toString().orEmpty()
            val confirmPassword = etConfirm.text?.toString().orEmpty()

            if (currentPassword.isBlank() || newPassword.isBlank() || confirmPassword.isBlank()) {
                Toast.makeText(this, "Please fill in all password fields", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            if (newPassword.length < 8) {
                Toast.makeText(this, "New password must be at least 8 characters", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            if (newPassword != confirmPassword) {
                Toast.makeText(this, "New password and confirm password must match", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }

            progress.visibility = View.VISIBLE
            btnSubmit.isEnabled = false
            Thread {
                val result = AuthApiService.changeOwnPassword(this, currentPassword, newPassword)
                runOnUiThread {
                    progress.visibility = View.GONE
                    btnSubmit.isEnabled = true
                    when (result) {
                        AdminActionResult.Success -> {
                            AuthStore.saveTokens(
                                this,
                                AuthStore.getAccessToken(this).orEmpty(),
                                refreshToken = null,
                                mustChangePassword = false
                            )
                            Toast.makeText(this, "Password changed successfully", Toast.LENGTH_LONG).show()
                            startActivity(Intent(this, HomeActivity::class.java))
                            finish()
                        }
                        is AdminActionResult.Error -> {
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                        }
                    }
                }
            }.start()
        }
    }

    override fun onBackPressed() {
        Toast.makeText(this, "Please change your password or log out", Toast.LENGTH_SHORT).show()
    }
}

