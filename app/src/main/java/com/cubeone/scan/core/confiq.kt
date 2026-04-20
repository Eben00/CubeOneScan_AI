// core/config/IntegrationConfig.kt
package com.cubeone.scan.core.config

object IntegrationConfig {
    // CRM Systems
    object CRM {
        const val ENABLED = true
        const val API_URL = "https://your-crm.com/api/v1/leads"
        const val API_KEY = "your-crm-api-key"
        const val TIMEOUT_MS = 30000L
    }

    // Trade-In Valuation APIs (e.g., Galves, KBB, Black Book)
    object TradeIn {
        const val ENABLED = true
        const val API_URL = "https://api.galves.com/tradein"
        const val API_KEY = "your-galves-api-key"
        const val TIMEOUT_MS = 30000L
    }

    // Inventory/DMS Systems
    object Inventory {
        const val ENABLED = true
        const val API_URL = "https://your-dms.com/api/inventory"
        const val API_KEY = "your-dms-api-key"
        const val TIMEOUT_MS = 30000L
    }

    // Webhook for custom integrations
    object Webhook {
        const val ENABLED = true
        const val URL = "https://your-webhook-endpoint.com/scan"
        const val SECRET = "your-webhook-secret"
    }
}