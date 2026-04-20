package com.cubeone.scan.ui

import android.os.Bundle
import android.text.Editable
import android.text.TextWatcher
import android.widget.ArrayAdapter
import android.widget.Button
import android.widget.EditText
import android.widget.ListView
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.cubeone.scan.R
import com.cubeone.scan.core.auth.AuditEvent
import com.cubeone.scan.core.auth.AuditEventsResult
import com.cubeone.scan.core.auth.AuthApiService

class AuditEventsActivity : AppCompatActivity() {
    private val events = mutableListOf<AuditEvent>()
    private lateinit var adapter: ArrayAdapter<String>

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_audit_events)

        val etAction = findViewById<EditText>(R.id.etAuditActionFilter)
        val etUser = findViewById<EditText>(R.id.etAuditUserFilter)
        val btnRefresh = findViewById<Button>(R.id.btnRefreshAudit)
        val listView = findViewById<ListView>(R.id.lvAuditEvents)

        adapter = ArrayAdapter(this, android.R.layout.simple_list_item_1, mutableListOf())
        listView.adapter = adapter

        val reload = {
            loadAuditEvents(
                actionFilter = etAction.text?.toString().orEmpty(),
                userFilter = etUser.text?.toString().orEmpty()
            )
        }

        val watcher = object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) = Unit
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) = Unit
            override fun afterTextChanged(s: Editable?) = reload()
        }
        etAction.addTextChangedListener(watcher)
        etUser.addTextChangedListener(watcher)
        btnRefresh.setOnClickListener { reload() }

        loadAuditEvents()
    }

    private fun loadAuditEvents(actionFilter: String = "", userFilter: String = "") {
        Thread {
            val result = AuthApiService.listAuditEvents(this, actionFilter, userFilter)
            runOnUiThread {
                when (result) {
                    is AuditEventsResult.Success -> {
                        events.clear()
                        events.addAll(result.events)
                        adapter.clear()
                        adapter.addAll(events.map { event ->
                            buildString {
                                append(event.ts.replace('T', ' ').removeSuffix("Z"))
                                append("\n")
                                append(event.type.ifBlank { "event" })
                                append("\nActor: ")
                                append(event.actorEmail.ifBlank { "-" })
                                append("   Target: ")
                                append(event.targetEmail.ifBlank { "-" })
                            }
                        })
                        adapter.notifyDataSetChanged()
                    }
                    is AuditEventsResult.Error -> {
                        Toast.makeText(this, result.message, Toast.LENGTH_LONG).show()
                    }
                }
            }
        }.start()
    }
}

