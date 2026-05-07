package com.cubeone.scan.ui

import android.os.Bundle
import android.view.LayoutInflater
import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.text.Editable
import android.text.TextWatcher
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.EditText
import android.widget.ListView
import android.widget.Toast
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AdminActionResult
import com.cubeone.scan.core.auth.AdminUser
import com.cubeone.scan.core.auth.AdminUsersResult
import com.cubeone.scan.core.auth.AuthApiService
import com.cubeone.scan.core.auth.AuthStore

class UserManagementActivity : AppCompatActivity() {
    private val users = mutableListOf<AdminUser>()
    private val filteredUsers = mutableListOf<AdminUser>()
    private lateinit var listView: ListView
    private lateinit var btnRefresh: Button
    private lateinit var btnCreateUser: Button
    private lateinit var etUserSearch: EditText
    private lateinit var adapter: ArrayAdapter<String>
    private var currentUserId: String = ""

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val role = normalizeBusinessRole(AuthStore.getRole(this).orEmpty())
        if (role != "dealer_principal" && role != "sales_manager") {
            Toast.makeText(this, "You are not allowed to manage users", Toast.LENGTH_LONG).show()
            finish()
            return
        }
        setContentView(R.layout.activity_user_management)

        listView = findViewById(R.id.lvUsers)
        btnRefresh = findViewById(R.id.btnRefreshUsers)
        btnCreateUser = findViewById(R.id.btnCreateUser)
        etUserSearch = findViewById(R.id.etUserSearch)
        currentUserId = AuthStore.getUserId(this).orEmpty()
        adapter = ArrayAdapter(this, android.R.layout.simple_list_item_1, mutableListOf())
        listView.adapter = adapter

        btnRefresh.setOnClickListener { loadUsers() }
        btnCreateUser.setOnClickListener { showCreateUserDialog() }
        etUserSearch.addTextChangedListener(object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) = Unit
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) = Unit
            override fun afterTextChanged(s: Editable?) {
                applyUserFilter(s?.toString().orEmpty())
            }
        })
        listView.setOnItemClickListener { _, _, position, _ ->
            val user = filteredUsers.getOrNull(position) ?: return@setOnItemClickListener
            showRoleDialog(user)
        }

        loadUsers()
    }

    private fun normalizeBusinessRole(rawRole: String): String {
        val role = rawRole.trim().lowercase()
        return when (role) {
            "dealer_principal", "sales_manager", "sales_person" -> role
            "superadmin", "owner", "admin", "tenant_admin_editor", "tenant_admin_approver" -> "dealer_principal"
            "manager", "sales-manager", "sales manager" -> "sales_manager"
            "agent" -> "sales_person"
            else -> "sales_person"
        }
    }

    private fun loadUsers() {
        btnRefresh.isEnabled = false
        Thread {
            val result = AuthApiService.listAdminUsers(this)
            runOnUiThread {
                btnRefresh.isEnabled = true
                when (result) {
                    is AdminUsersResult.Success -> {
                        users.clear()
                        users.addAll(result.users)
                        applyUserFilter(etUserSearch.text?.toString().orEmpty())
                    }
                    is AdminUsersResult.Error -> {
                        Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                    }
                }
            }
        }.start()
    }

    private fun showCreateUserDialog() {
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_create_user, null)
        val etEmail = view.findViewById<EditText>(R.id.etNewUserEmail)
        val etPassword = view.findViewById<EditText>(R.id.etNewUserPassword)
        val etRole = view.findViewById<EditText>(R.id.etNewUserRole)
        etRole.setText("sales_person")

        AlertDialog.Builder(this)
            .setTitle("Create user (this dealership)")
            .setView(view)
            .setNegativeButton("Cancel", null)
            .setPositiveButton("Create") { _, _ ->
                val email = etEmail.text?.toString().orEmpty().trim()
                val password = etPassword.text?.toString().orEmpty()
                val role = etRole.text?.toString().orEmpty().trim().ifBlank { "sales_person" }
                if (email.isBlank() || password.length < 8) {
                    Toast.makeText(this, "Email required and password must be at least 8 chars", Toast.LENGTH_LONG).show()
                    return@setPositiveButton
                }
                Thread {
                    when (val result = AuthApiService.createAdminUser(this, email, password, role)) {
                        is AdminActionResult.Success -> runOnUiThread {
                            Toast.makeText(this, "User created", Toast.LENGTH_SHORT).show()
                            loadUsers()
                        }
                        is AdminActionResult.Error -> runOnUiThread {
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                        }
                    }
                }.start()
            }
            .show()
    }

    private fun showRoleDialog(user: AdminUser) {
        val actions = mutableListOf("Change role")
        val canToggleStatus = user.userId != currentUserId
        if (canToggleStatus) {
            actions += if (user.active) "Deactivate user" else "Activate user"
        }
        if (!user.lockedUntil.isNullOrBlank()) {
            actions += "Unlock account"
        }
        actions += "Reset password"
        AlertDialog.Builder(this)
            .setTitle("Manage ${user.email}")
            .setItems(actions.toTypedArray()) { _, which ->
                when (actions[which]) {
                    "Change role" -> showChangeRoleDialog(user)
                    "Deactivate user" -> updateUserStatus(user, false)
                    "Activate user" -> updateUserStatus(user, true)
                    "Unlock account" -> unlockUser(user)
                    "Reset password" -> showResetPasswordDialog(user)
                }
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun showChangeRoleDialog(user: AdminUser) {
        val roles = arrayOf("dealer_principal", "sales_manager", "sales_person")
        AlertDialog.Builder(this)
            .setTitle("Change role for ${user.email}")
            .setItems(roles) { _, which ->
                val role = roles[which]
                Thread {
                    when (val result = AuthApiService.updateUserRole(this, user.userId, role)) {
                        is AdminActionResult.Success -> runOnUiThread {
                            Toast.makeText(this, "Role updated to $role", Toast.LENGTH_SHORT).show()
                            loadUsers()
                        }
                        is AdminActionResult.Error -> runOnUiThread {
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                        }
                    }
                }.start()
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    private fun updateUserStatus(user: AdminUser, active: Boolean) {
        Thread {
            when (val result = AuthApiService.updateUserStatus(this, user.userId, active)) {
                is AdminActionResult.Success -> runOnUiThread {
                    val status = if (active) "activated" else "deactivated"
                    Toast.makeText(this, "User $status", Toast.LENGTH_SHORT).show()
                    loadUsers()
                }
                is AdminActionResult.Error -> runOnUiThread {
                    Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                }
            }
        }.start()
    }

    private fun showResetPasswordDialog(user: AdminUser) {
        val input = EditText(this).apply {
            hint = "New password (min 8 chars)"
            inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
        }
        val generated = generateTemporaryPassword()
        input.setText(generated)
        AlertDialog.Builder(this)
            .setTitle("Reset password for ${user.email}")
            .setView(input)
            .setNeutralButton("Copy temp password") { _, _ ->
                copyToClipboard("temp_password_${user.email}", input.text?.toString().orEmpty())
                Toast.makeText(this, "Temporary password copied", Toast.LENGTH_SHORT).show()
            }
            .setNegativeButton("Cancel", null)
            .setPositiveButton("Reset") { _, _ ->
                val newPassword = input.text?.toString().orEmpty()
                if (newPassword.length < 8) {
                    Toast.makeText(this, "Password must be at least 8 chars", Toast.LENGTH_LONG).show()
                    return@setPositiveButton
                }
                Thread {
                    when (val result = AuthApiService.resetUserPassword(this, user.userId, newPassword)) {
                        is AdminActionResult.Success -> runOnUiThread {
                            Toast.makeText(this, "Password reset complete", Toast.LENGTH_SHORT).show()
                        }
                        is AdminActionResult.Error -> runOnUiThread {
                            Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                        }
                    }
                }.start()
            }
            .show()
    }

    private fun unlockUser(user: AdminUser) {
        Thread {
            val result = AuthApiService.unlockUser(this, user.userId)
            runOnUiThread {
                when (result) {
                    AdminActionResult.Success -> {
                        Toast.makeText(this, "Account unlocked for ${user.email}", Toast.LENGTH_LONG).show()
                        loadUsers()
                    }
                    is AdminActionResult.Error -> {
                        Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                    }
                }
            }
        }.start()
    }

    private fun applyUserFilter(rawQuery: String) {
        val query = rawQuery.trim().lowercase()
        filteredUsers.clear()
        if (query.isBlank()) {
            filteredUsers.addAll(users)
        } else {
            filteredUsers.addAll(
                users.filter { user ->
                    val status = if (user.active) "active" else "inactive"
                    "${user.email} ${user.role} $status ${user.dealerId}"
                        .lowercase()
                        .contains(query)
                }
            )
        }
        adapter.clear()
        adapter.addAll(filteredUsers.map { formatUserLine(it) })
        adapter.notifyDataSetChanged()
    }

    private fun formatUserLine(user: AdminUser): String {
        val status = if (user.active) "Active" else "Inactive"
        val locked = !user.lockedUntil.isNullOrBlank()
        val lockTag = if (locked) " • LOCKED" else ""
        return "${user.email}\nRole: ${user.role} • $status$lockTag • Dealer: ${user.dealerId}"
    }

    private fun generateTemporaryPassword(): String {
        val stamp = (System.currentTimeMillis() % 100000).toString().padStart(5, '0')
        return "Temp@$stamp"
    }

    private fun copyToClipboard(label: String, text: String) {
        val cm = getSystemService(Context.CLIPBOARD_SERVICE) as ClipboardManager
        cm.setPrimaryClip(ClipData.newPlainText(label, text))
    }
}

