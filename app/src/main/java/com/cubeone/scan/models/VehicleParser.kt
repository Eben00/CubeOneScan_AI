package com.cubeone.scan.models

import android.util.Log

object VehicleParser {
    private const val TAG = "VehicleParser"

    fun parse(raw: String): VehicleData? {
        Log.d(TAG, "RAW: ${raw.take(50)}...")

        val parts = raw.split("%").filter { it.isNotBlank() }
        Log.d(TAG, "PARTS: ${parts.size}")

        if (parts.isEmpty()) {
            Log.e(TAG, "No usable vehicle parts found")
            return null
        }

        fun part(i: Int): String = parts.getOrNull(i)?.trim().orEmpty()
        val vinPattern = Regex("^[A-HJ-NPR-Z0-9]{17}$")
        val vinDetected = parts.firstOrNull { vinPattern.matches(it.trim().uppercase()) }?.trim().orEmpty()
        val vin = if (part(11).isNotBlank()) part(11) else vinDetected
        val estimatedYear = decodeVinYear(vin)
        val firstRegistrationDate = if (estimatedYear != null) "$estimatedYear-01-01" else ""

        val makeRaw = part(8)
        val modelRaw = part(9)
        val (valuationModel, valuationVariant) = splitForValuation(makeRaw, modelRaw)

        return VehicleData(
            registration = part(5),
            licenceNumber = part(6),
            make = makeRaw,
            model = modelRaw,
            valuationModel = valuationModel,
            valuationVariant = valuationVariant,
            color = part(10),
            vin = vin,
            engineNumber = part(12),
            expiry = part(13),
            firstRegistrationDate = firstRegistrationDate,
            firstRegistrationYear = estimatedYear?.toString().orEmpty(),
            rawPayload = raw
        )
    }

    /**
     * SA licence disc often stores one long "model" string like "HONDA JAZZ 1,5 EX A/T".
     * Valuation flow expects separate model line (JAZZ) and catalog variant (1.5I EX AT).
     */
    private fun splitForValuation(makeRaw: String, modelRaw: String): Pair<String, String> {
        val make = makeRaw.trim()
        var combined = modelRaw.trim().replace(',', '.')
        if (combined.isEmpty()) return "" to ""

        if (make.isNotEmpty() && combined.startsWith(make, ignoreCase = true)) {
            val rest = combined.substring(make.length).trim()
            if (rest.isNotEmpty()) combined = rest
        }

        val tokens = combined.split(Regex("\\s+")).filter { it.isNotBlank() }
        if (tokens.isEmpty()) return "" to ""
        if (tokens.size == 1) return tokens[0] to ""

        // Model is usually the words before the first displacement token (e.g. 3.6, 2.0D).
        // This keeps multi-word models like "GRAND CHEROKEE" intact.
        val firstNumericIdx = tokens.indexOfFirst { it.any(Char::isDigit) }
        if (firstNumericIdx <= 0) {
            return "" to ""
        }
        val modelBase = tokens.take(firstNumericIdx).joinToString(" ")
        val variantRaw = tokens.drop(firstNumericIdx).joinToString(" ")
        val variantNorm = normalizeValuationVariant(variantRaw)
        return modelBase to variantNorm
    }

    private fun normalizeValuationVariant(fragment: String): String {
        var v = fragment.trim().replace(',', '.')
        v = v.replace(Regex("(?i)A\\s*/\\s*T\\b"), "AT")
        v = v.replace(Regex("(?i)\\bMT\\s*/\\s*M\\b"), "MTM")
        // Catalog uses "1.5I EX ..." style — insert I after engine displacement when missing.
        v = v.replace(Regex("(\\d+\\.\\d+)\\s+EX-S", RegexOption.IGNORE_CASE), "$1I EX-S")
        v = v.replace(Regex("(\\d+\\.\\d+)\\s+EX(?!-)", RegexOption.IGNORE_CASE), "$1I EX")
        v = v.replace(Regex("\\s+"), " ").trim()
        return v
    }

    private fun decodeVinYear(vin: String): Int? {
        if (vin.length != 17) return null
        val code = vin[9].uppercaseChar()
        val map = mapOf(
            'A' to 2010, 'B' to 2011, 'C' to 2012, 'D' to 2013, 'E' to 2014,
            'F' to 2015, 'G' to 2016, 'H' to 2017, 'J' to 2018, 'K' to 2019,
            'L' to 2020, 'M' to 2021, 'N' to 2022, 'P' to 2023, 'R' to 2024,
            'S' to 2025, 'T' to 2026, 'V' to 2027, 'W' to 2028, 'X' to 2029,
            'Y' to 2030, '1' to 2031, '2' to 2032, '3' to 2033, '4' to 2034,
            '5' to 2035, '6' to 2036, '7' to 2037, '8' to 2038, '9' to 2039
        )
        // Use previous 30-year cycle for older vehicles.
        val candidate = map[code] ?: return null
        val currentYear = java.time.LocalDate.now().year
        return if (candidate > currentYear + 1) candidate - 30 else candidate
    }
}