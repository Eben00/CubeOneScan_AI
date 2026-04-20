package com.cubeone.scan.ui

import android.content.Intent
import android.graphics.Color
import android.os.Bundle
import android.view.View
import android.widget.TextView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuthApiService
import com.cubeone.scan.core.auth.AuthResult
import com.cubeone.scan.core.auth.RegisterResult
import com.cubeone.scan.core.auth.AuthStore
import com.google.android.material.button.MaterialButton
import com.google.android.material.progressindicator.CircularProgressIndicator
import com.google.android.material.textfield.TextInputEditText

class LoginActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_login)

        val etEmail = findViewById<TextInputEditText>(R.id.etEmail)
        val etPassword = findViewById<TextInputEditText>(R.id.etPassword)
        val btnLogin = findViewById<MaterialButton>(R.id.btnLogin)
        val btnRegister = findViewById<MaterialButton>(R.id.btnRegister)
        val btnCheckAuthConnection = findViewById<MaterialButton>(R.id.btnCheckAuthConnection)
        val btnContinueOffline = findViewById<MaterialButton>(R.id.btnContinueOffline)
        val tvAuthStatus = findViewById<TextView>(R.id.tvAuthStatus)
        val progress = findViewById<CircularProgressIndicator?>(R.id.progressLogin)

        // Already logged in? Go straight to home.
        AuthStore.getAccessToken(this)?.let {
            navigateToHome()
            return
        }

        btnLogin.setOnClickListener {
            val email = etEmail.text?.toString().orEmpty()
            val password = etPassword.text?.toString().orEmpty()
            if (email.isBlank() || password.isBlank()) {
                Toast.makeText(this, "Enter email and password", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            progress?.visibility = View.VISIBLE
            btnLogin.isEnabled = false
            Thread {
                val result = AuthApiService.login(this, email, password)
                runOnUiThread {
                    progress?.visibility = View.GONE
                    btnLogin.isEnabled = true
                    when (result) {
                        is AuthResult.Success -> {
                            val senderName = email.substringBefore("@").ifBlank { result.userId.orEmpty() }
                            val dealershipName = result.dealerId
                                ?.takeIf { it.isNotBlank() }
                                ?.let { "Dealer $it" }
                                ?: "Dealership"
                            AuthStore.saveTokens(
                                this,
                                result.accessToken,
                                result.refreshToken,
                                userId = result.userId,
                                dealerId = result.dealerId,
                                branchId = result.branchId,
                                role = result.role,
                                userEmail = email,
                                displayName = senderName,
                                dealershipName = dealershipName,
                                mustChangePassword = result.mustChangePassword
                            )
                            if (result.mustChangePassword) {
                                startActivity(Intent(this, ChangePasswordActivity::class.java))
                                finish()
                            } else {
                                navigateToHome()
                            }
                        }
                        is AuthResult.Error -> {
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                        }
                    }
                }
            }.start()
        }

        btnRegister.setOnClickListener {
            val email = etEmail.text?.toString().orEmpty()
            val password = etPassword.text?.toString().orEmpty()
            if (email.isBlank() || password.isBlank()) {
                Toast.makeText(this, "Enter email and password to register", Toast.LENGTH_LONG).show()
                return@setOnClickListener
            }
            progress?.visibility = View.VISIBLE
            btnRegister.isEnabled = false
            Thread {
                val result = AuthApiService.register(this, email, password)
                runOnUiThread {
                    progress?.visibility = View.GONE
                    btnRegister.isEnabled = true
                    when (result) {
                        RegisterResult.Success ->
                            Toast.makeText(this, "Account created, please sign in", Toast.LENGTH_LONG).show()
                        is RegisterResult.Error ->
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                    }
                }
            }.start()
        }

        btnContinueOffline.setOnClickListener {
            // Local demo token lets HomeActivity pass auth checks without backend auth.
            AuthStore.saveTokens(this, "offline-demo-token", null)
            val savedToken = AuthStore.getAccessToken(this).orEmpty()
            Toast.makeText(
                this,
                "Demo mode enabled (token saved: ${savedToken.isNotBlank()})",
                Toast.LENGTH_SHORT
            ).show()
            navigateToHome()
        }

        btnCheckAuthConnection.setOnClickListener {
            refreshAuthStatus(tvAuthStatus)
        }

        refreshAuthStatus(tvAuthStatus)
    }

    private fun navigateToHome() {
        startActivity(Intent(this, HomeActivity::class.java))
        finish()
    }

    private fun refreshAuthStatus(statusView: TextView) {
        val baseUrl = AuthApiService.getAuthBaseUrl(this)
        statusView.text = "Auth: checking $baseUrl"
        statusView.setTextColor(Color.parseColor("#FF9E9E9E"))

        Thread {
            val reachable = AuthApiService.isAuthServerReachable(this)
            runOnUiThread {
                if (reachable) {
                    statusView.text = "Auth: connected ($baseUrl)"
                    statusView.setTextColor(Color.parseColor("#FF2E7D32"))
                } else {
                    statusView.text = "Auth: disconnected ($baseUrl)"
                    statusView.setTextColor(Color.parseColor("#FFC62828"))
                }
            }
        }.start()
    }
}

