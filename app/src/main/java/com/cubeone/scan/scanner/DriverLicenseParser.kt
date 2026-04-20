package com.cubeone.scan.scanner

import android.util.Log
import android.util.Base64
import java.math.BigInteger

object DriverLicenseParser {

    private const val TAG = "DriverLicense"

    fun parse(data: ByteArray): Map<String, String> {
        val result = mutableMapOf<String, String>()

        try {
            Log.d(TAG, "=== Parsing ${data.size} bytes ===")

            // Extract ID first (we need it to validate dates)
            val idNumber = extractIdNumber(data)
            result["ID_NUMBER"] = idNumber

            // Parse the ASN.1 structure properly to get all fields
            val parsedFields = parseASN1Fields(data)

            // Assign text fields
            result["SURNAME"] = parsedFields.surname ?: ""
            result["NAMES"] = parsedFields.names ?: ""
            result["LICENSE_NUMBER"] = parsedFields.licenseNumber ?: extractLicenseNumberFallback(data).orEmpty()
            result["GENDER"] = parsedFields.gender ?: "Unknown"

            // Fallback parser for common SA block-1 format used in decrypted payloads.
            // This improves surname/names/license extraction when ASN.1-style reads are sparse.
            val legacy = parseLegacyBlock1(data)
            if (result["SURNAME"].isNullOrBlank() && !legacy["SURNAME"].isNullOrBlank()) {
                result["SURNAME"] = legacy["SURNAME"].orEmpty()
            }
            if (result["NAMES"].isNullOrBlank() && !legacy["NAMES"].isNullOrBlank()) {
                result["NAMES"] = legacy["NAMES"].orEmpty()
            }
            if (result["LICENSE_NUMBER"].isNullOrBlank() && !legacy["LICENSE_NUMBER"].isNullOrBlank()) {
                result["LICENSE_NUMBER"] = legacy["LICENSE_NUMBER"].orEmpty()
            }
            if ((result["GENDER"].isNullOrBlank() || result["GENDER"] == "Unknown") && !legacy["GENDER"].isNullOrBlank()) {
                result["GENDER"] = legacy["GENDER"].orEmpty()
            }

            val normalizedIdentity = normalizeIdentityNameFields(
                surnameRaw = result["SURNAME"].orEmpty(),
                namesRaw = result["NAMES"].orEmpty()
            )
            result["SURNAME"] = normalizedIdentity.first
            result["NAMES"] = normalizedIdentity.second

            // Accurate portrait path: extract embedded image bytes from decrypted payload.
            result["PHOTO"] = extractEmbeddedPhotoBase64(data).orEmpty()
            val hasLegacyDates =
                legacy["DOB"]?.isNotBlank() == true &&
                legacy["ISSUE_DATE"]?.isNotBlank() == true &&
                legacy["EXPIRY_DATE"]?.isNotBlank() == true

            if (hasLegacyDates) {
                // Accurate path: when section2 decode yields dates, treat those as authoritative.
                result["DOB"] = legacy["DOB"].orEmpty()
                result["ISSUE_DATE"] = legacy["ISSUE_DATE"].orEmpty()
                result["EXPIRY_DATE"] = legacy["EXPIRY_DATE"].orEmpty()
            } else {
                // Fallback path: infer dates from ASN.1/BCD scans.
                val dates = extractDatesFromASN1(data, parsedFields.rawDateBytes)

                if (dates.isNotEmpty()) {
                    val sortedDates = dates.sorted()
                    val expectedDobFromId = parseDateFromId(idNumber)

                    val dob = if (expectedDobFromId != null) {
                        dates.minByOrNull { kotlin.math.abs(parseDateToDays(it) - parseDateToDays(expectedDobFromId)) } ?: sortedDates.first()
                    } else {
                        sortedDates.first()
                    }

                    val dobLooksValid = expectedDobFromId == null ||
                        kotlin.math.abs(parseDateToDays(dob) - parseDateToDays(expectedDobFromId)) <= (365L * 2L)

                    if (dobLooksValid) {
                        val remainingDates = dates.filter { it != dob }.sorted()
                        val pair = findBestIssueExpiryPair(remainingDates, dob)

                        result["DOB"] = dob
                        result["ISSUE_DATE"] = pair.first
                        result["EXPIRY_DATE"] = pair.second
                    } else {
                        result["DOB"] = expectedDobFromId ?: ""
                        result["ISSUE_DATE"] = ""
                        result["EXPIRY_DATE"] = ""
                    }
                } else {
                    result["DOB"] = parseDateFromId(idNumber) ?: ""
                    result["ISSUE_DATE"] = ""
                    result["EXPIRY_DATE"] = ""
                }
            }

            Log.d(TAG, "FINAL: ID=${result["ID_NUMBER"]}, " +
                    "Name=${result["SURNAME"]} ${result["NAMES"]}, " +
                    "DOB=${result["DOB"]}, " +
                    "Issue=${result["ISSUE_DATE"]}, " +
                    "Expiry=${result["EXPIRY_DATE"]}")

        } catch (e: Exception) {
            Log.e(TAG, "Parse error: ${e.message}", e)
        }

        return result
    }

    private fun extractLicenseNumberFallback(data: ByteArray): String? {
        val text = data.toString(Charsets.ISO_8859_1)
            .replace(Regex("[^A-Za-z0-9]"), " ")
            .uppercase()

        val exact = Regex("""\b\d{10}[A-Z]{2}\b""").find(text)?.value
        if (!exact.isNullOrBlank()) return exact

        return Regex("""\b\d{10,12}[A-Z]{1,2}\b""")
            .find(text)
            ?.value
    }

    private fun normalizeIdentityNameFields(surnameRaw: String, namesRaw: String): Pair<String, String> {
        fun clean(value: String): String = value
            .trim()
            .replace(Regex("\\s+"), " ")

        fun splitWords(value: String): List<String> =
            clean(value).split(" ").filter { it.isNotBlank() }

        fun looksLikeInitials(value: String): Boolean {
            val token = clean(value).replace(".", "")
            return token.length in 1..4 && token.all { it.isLetter() }
        }

        var surname = clean(surnameRaw)
        var names = clean(namesRaw)

        if (surname.isBlank() && names.isNotBlank()) {
            val words = splitWords(names)
            if (words.size >= 2 && looksLikeInitials(words.first())) {
                return words.drop(1).joinToString(" ") to words.first()
            }
            return surname to names
        }

        if (surname.isNotBlank() && names.isNotBlank()) {
            // Typical bad read: both fields contain "LF DEGENAAR".
            if (surname.equals(names, ignoreCase = true)) {
                val words = splitWords(surname)
                if (words.size >= 2 && looksLikeInitials(words.first())) {
                    return words.drop(1).joinToString(" ") to words.first()
                }
            }

            // If names still includes surname tail, keep only initials/given part.
            if (names.endsWith(" $surname", ignoreCase = true)) {
                names = names.substring(0, names.length - surname.length).trim()
            }

            // If surname starts with initials prefix, strip it from surname.
            if (names.isNotBlank() && looksLikeInitials(names) && surname.startsWith("$names ", ignoreCase = true)) {
                surname = surname.substring(names.length).trim()
            }
        }

        return surname to names
    }

    private fun findBestIssueExpiryPair(candidates: List<String>, dob: String): Pair<String, String> {
        if (candidates.size < 2) return "" to ""

        val dobDays = parseDateToDays(dob)
        val nowDays = parseDateToDays("2035-12-31") // upper sanity ceiling, not current-clock dependent

        var bestIssue = ""
        var bestExpiry = ""
        var bestScore = Long.MAX_VALUE

        for (i in candidates.indices) {
            for (j in candidates.indices) {
                if (i == j) continue
                val issue = candidates[i]
                val expiry = candidates[j]
                if (issue >= expiry) continue

                val issueDays = parseDateToDays(issue)
                val expiryDays = parseDateToDays(expiry)
                if (issueDays <= dobDays + (365L * 16L)) continue // issue date should be adult age+
                if (expiryDays > nowDays) continue // filter obvious corrupted far-future values

                val validityYears = (expiryDays - issueDays) / 365L
                val score = kotlin.math.abs(validityYears - 5L) // SA licences commonly renew around this range

                if (score < bestScore) {
                    bestScore = score
                    bestIssue = issue
                    bestExpiry = expiry
                }
            }
        }

        return bestIssue to bestExpiry
    }

    private fun parseLegacyBlock1(data: ByteArray): Map<String, String> {
        val out = mutableMapOf<String, String>()
        if (data.size < 128) return out

        try {
            val block = data.copyOfRange(0, 128)
            val section1Len = block[10].toInt() and 0xFF
            val section2Len = block[12].toInt() and 0xFF
            val section1Start = 15
            val section1EndInclusive = (section1Start + section1Len).coerceAtMost(block.size - 1)

            if (section1Start < block.size && section1EndInclusive >= section1Start) {
                val sb = StringBuilder()
                for (i in section1Start..section1EndInclusive) {
                    val v = block[i].toInt() and 0xFF
                    when (v) {
                        0xE0 -> sb.append(",")
                        0xE1 -> sb.append("[]")
                        else -> sb.append((v.toChar()))
                    }
                }
                val section1 = sb.toString()
                    .replace("[][][]", ",,,,")
                    .replace("[][]", ",,,")
                    .replace("[]", ",,")

                val fields = section1.split(",")
                // Known field positions from legacy decoder references.
                if (fields.size > 4) out["SURNAME"] = fields[4].trim()
                if (fields.size > 5) out["NAMES"] = fields[5].trim()
                if (fields.size > 13) out["LICENSE_NUMBER"] = fields[13].trim()
            }

            // Optional section2 decode for issue/expiry/dob/gender fallback.
            // This mirrors the legacy B4X logic as closely as possible.
            val section2Start = section1Start + section1Len
            val section2End = (section2Start + section2Len - 1).coerceAtMost(block.size - 1)
            if (section2Start in block.indices && section2End >= section2Start) {
                val section2 = buildString {
                    for (i in section2Start..section2End) {
                        val h = String.format("%02x", block[i].toInt() and 0xFF)
                        if (i == section2Start) {
                            // First byte is appended as-is in legacy code.
                            append(h)
                        } else {
                            val c1 = if (h[0] == 'a') '.' else h[0]
                            val c2 = if (h[1] == 'a') '.' else h[1]
                            append(c1)
                            append(c2)
                        }
                    }
                }.removeSuffix(".")

                val amended = buildString {
                    for (ch in section2) {
                        if (ch == '.') append("........") else append(ch)
                    }
                }

                // Legacy field offsets from working reference decoder.
                if (amended.length >= 72) {
                    val issueDate1 = amended.substring(2, 10)
                    val birthDate = amended.substring(46, 54)
                    val validFrom = amended.substring(54, 62)
                    val validTo = amended.substring(62, 70)
                    val genderCode = amended.substring(70, 72)

                    out["DOB"] = normalizeYyyyMmDd(birthDate)
                    // For app display: Valid From/To map to ISSUE/EXPIRY fields.
                    out["ISSUE_DATE"] = normalizeYyyyMmDd(validFrom)
                    out["EXPIRY_DATE"] = normalizeYyyyMmDd(validTo)
                    // Preserve issueDate1 only as last-resort fallback if validFrom is blank.
                    if (out["ISSUE_DATE"].isNullOrBlank()) {
                        out["ISSUE_DATE"] = normalizeYyyyMmDd(issueDate1)
                    }
                    out["GENDER"] = when (genderCode) {
                        "01" -> "Male"
                        "02" -> "Female"
                        else -> out["GENDER"].orEmpty()
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Legacy block parse skipped: ${e.message}")
        }

        return out
    }

    private fun normalizeYyyyMmDd(value: String): String {
        val v = value.trim()
        if (!v.matches(Regex("""\d{8}"""))) return ""
        val y = v.substring(0, 4).toIntOrNull() ?: return ""
        val m = v.substring(4, 6).toIntOrNull() ?: return ""
        val d = v.substring(6, 8).toIntOrNull() ?: return ""
        if (y !in 1900..2099 || m !in 1..12 || d !in 1..31) return ""
        return "$y-${"%02d".format(m)}-${"%02d".format(d)}"
    }

    private fun extractEmbeddedPhotoBase64(data: ByteArray): String? {
        val maxBytes = 160 * 1024 // keep payload small for Intent transfer

        fun tryJpeg(): ByteArray? {
            var start = -1
            var i = 0
            while (i < data.size - 1) {
                val b1 = data[i].toInt() and 0xFF
                val b2 = data[i + 1].toInt() and 0xFF
                if (start < 0 && b1 == 0xFF && b2 == 0xD8) {
                    start = i
                    i += 2
                    continue
                }
                if (start >= 0 && b1 == 0xFF && b2 == 0xD9) {
                    val end = i + 2
                    val len = end - start
                    if (len in 512..maxBytes) {
                        return data.copyOfRange(start, end)
                    }
                    start = -1
                }
                i++
            }
            return null
        }

        fun tryPng(): ByteArray? {
            val pngSig = byteArrayOf(
                0x89.toByte(), 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A
            )
            for (i in 0..(data.size - pngSig.size)) {
                var ok = true
                for (j in pngSig.indices) {
                    if (data[i + j] != pngSig[j]) {
                        ok = false
                        break
                    }
                }
                if (!ok) continue
                // Find IEND chunk terminator
                val endMarker = byteArrayOf(0x49, 0x45, 0x4E, 0x44, 0xAE.toByte(), 0x42, 0x60, 0x82.toByte())
                for (k in i + pngSig.size until data.size - endMarker.size) {
                    var endOk = true
                    for (m in endMarker.indices) {
                        if (data[k + m] != endMarker[m]) {
                            endOk = false
                            break
                        }
                    }
                    if (endOk) {
                        val end = k + endMarker.size
                        val len = end - i
                        if (len in 512..maxBytes) {
                            return data.copyOfRange(i, end)
                        }
                        break
                    }
                }
            }
            return null
        }

        val imageBytes = tryJpeg() ?: tryPng() ?: return null
        return Base64.encodeToString(imageBytes, Base64.NO_WRAP)
    }

    data class ParsedFields(
        val surname: String? = null,
        val names: String? = null,
        val licenseNumber: String? = null,
        val gender: String? = null,
        val rawDateBytes: List<ByteArray> = emptyList()
    )

    private fun parseASN1Fields(data: ByteArray): ParsedFields {
        var surname: String? = null
        var names: String? = null
        var licenseNumber: String? = null
        var gender: String? = null
        val dateBytes = mutableListOf<ByteArray>()
        val strings = mutableListOf<String>()

        var offset = 0

        try {
            // Skip leading nulls
            while (offset < data.size && data[offset] == 0x00.toByte()) offset++

            // Check for SEQUENCE
            if (offset >= data.size || data[offset] != 0x30.toByte()) {
                return ParsedFields()
            }

            offset++ // Skip 0x30
            val seqLength = readASN1Length(data, offset)
            val lengthBytes = asn1LengthByteCount(data, offset)
            offset += lengthBytes

            var fieldCount = 0
            while (offset < data.size && fieldCount < 30) {
                fieldCount++

                val tag = data[offset].toInt() and 0xFF

                if (tag == 0x00 || tag == 0xFF) {
                    offset++
                    continue
                }

                val length = readASN1Length(data, offset + 1)
                val lenBytes = asn1LengthByteCount(data, offset + 1)
                val contentStart = offset + 1 + lenBytes
                val contentEnd = contentStart + length

                if (contentEnd > data.size || length < 0) break

                val content = data.copyOfRange(contentStart, contentEnd)

                when (tag) {
                    0x02 -> { // INTEGER - could be gender or ID
                        if (length == 1) {
                            when (content[0].toInt() and 0xFF) {
                                1 -> gender = "Male"
                                2 -> gender = "Female"
                            }
                        } else if (length > 4 && length <= 20) {
                            // Might be ID as integer
                            val bigInt = BigInteger(1, content).toString()
                            if (bigInt.length == 13 && licenseNumber == null) {
                                licenseNumber = bigInt
                            }
                        }
                    }

                    0x04, 0x13, 0x16 -> { // OCTET STRING, PrintableString, IA5String
                        val str = content.toString(Charsets.ISO_8859_1).trim()
                            .replace(Regex("[\\x00-\\x1F]"), "")

                        if (str.isNotEmpty()) {
                            // Check for license pattern
                            if (str.matches(Regex("""\d{10,12}[A-Z]{1,2}"""))) {
                                licenseNumber = str
                            }
                            // Check for date-like bytes embedded in string (some licenses encode dates as strings)
                            else if (str.matches(Regex("""\d{2}[\-/]\d{2}[\-/]\d{4}"""))) {
                                // Already formatted date string
                                strings.add(str)
                            }
                            // Regular text
                            else if (str.all { it.isLetter() || it.isWhitespace() || it == ',' || it == '-' }) {
                                strings.add(str)
                            }
                        }

                        // Check if content contains BCD dates (3 bytes)
                        if (length >= 3) {
                            for (i in 0..length-3) {
                                if (isValidBCDDate(content, i)) {
                                    dateBytes.add(content.copyOfRange(i, i+3))
                                }
                            }
                        }
                    }

                    // Context-specific tags often contain dates in SA licenses
                    in 0xA0..0xAF -> {
                        if (length == 3 && isValidBCDDate(content, 0)) {
                            dateBytes.add(content.copyOfRange(0, 3))
                        } else if (length == 1) {
                            when (content[0].toInt() and 0xFF) {
                                0, 1, 'M'.code -> gender = "Male"
                                2, 'F'.code -> gender = "Female"
                            }
                        } else {
                            val str = content.toString(Charsets.ISO_8859_1).trim()
                            if (str.isNotEmpty() && str.all { it.isLetter() || it.isWhitespace() }) {
                                strings.add(str)
                            }
                        }
                    }
                }

                offset = contentEnd
            }

            // Assign strings: first is surname, second is names
            if (strings.isNotEmpty()) {
                surname = strings[0]
                if (strings.size > 1) {
                    names = strings[1]
                }
            }

        } catch (e: Exception) {
            Log.e(TAG, "ASN.1 parse error: ${e.message}")
        }

        return ParsedFields(surname, names, licenseNumber, gender, dateBytes)
    }

    private fun extractDatesFromASN1(data: ByteArray, rawDateBytes: List<ByteArray>): List<String> {
        val dates = mutableListOf<String>()

        // First try the date bytes found in ASN.1 fields
        for (bytes in rawDateBytes) {
            val dateStr = bcdToDateString(bytes)
            if (dateStr != null && !dates.contains(dateStr)) {
                dates.add(dateStr)
            }
        }

        // If we didn't find enough dates, scan specific regions
        if (dates.size < 3) {
            // Look for dates after the text fields (typically in the second half of data)
            val startScan = data.size / 3
            val endScan = data.size - 3

            for (i in startScan until endScan) {
                if (isValidBCDDate(data, i)) {
                    val dateStr = bcdToDateString(data.copyOfRange(i, i+3))
                    if (dateStr != null && !dates.contains(dateStr)) {
                        // Validate it's a reasonable date (1900-2050)
                        val year = dateStr.substring(0, 4).toInt()
                        if (year in 1900..2050) {
                            dates.add(dateStr)
                        }
                    }
                }
            }
        }

        return dates.sorted()
    }

    private fun extractIdNumber(data: ByteArray): String {
        // Try ASN.1 first
        var offset = 0
        while (offset < data.size - 15) {
            if (data[offset] == 0x30.toByte()) break
            offset++
        }

        // If not found, scan for 13 consecutive digits
        val text = data.toString(Charsets.ISO_8859_1)
        val match = Regex("""\d{13}""").find(text)
        if (match != null) return match.value

        // Fallback to byte range
        return data.copyOfRange(55, 68)
            .toString(Charsets.ISO_8859_1)
            .filter { it.isDigit() }
            .take(13)
    }

    private fun parseDateFromId(idNumber: String): String? {
        if (idNumber.length != 13) return null

        return try {
            val year = idNumber.substring(0, 2).toInt()
            val month = idNumber.substring(2, 4).toInt()
            val day = idNumber.substring(4, 6).toInt()

            // Determine century (00-49 = 2000s, 50-99 = 1900s for driver's licenses)
            val fullYear = if (year < 50) 2000 + year else 1900 + year

            if (month in 1..12 && day in 1..31) {
                String.format("%04d-%02d-%02d", fullYear, month, day)
            } else null
        } catch (e: Exception) {
            null
        }
    }

    private fun parseDateToDays(dateStr: String): Long {
        return try {
            val parts = dateStr.split("-")
            val year = parts[0].toLong()
            val month = parts[1].toLong()
            val day = parts[2].toLong()
            year * 365 + month * 30 + day // Approximate for comparison
        } catch (e: Exception) {
            0
        }
    }

    private fun isValidBCDDate(data: ByteArray, offset: Int): Boolean {
        if (offset + 3 > data.size) return false

        val b1 = data[offset].toInt() and 0xFF
        val b2 = data[offset + 1].toInt() and 0xFF
        val b3 = data[offset + 2].toInt() and 0xFF

        // Check if valid BCD
        if ((b1 and 0x0F) > 9 || ((b1 shr 4) and 0x0F) > 9) return false
        if ((b2 and 0x0F) > 9 || ((b2 shr 4) and 0x0F) > 9) return false
        if ((b3 and 0x0F) > 9 || ((b3 shr 4) and 0x0F) > 9) return false

        // Check if valid date
        val month = ((b2 shr 4) * 10) + (b2 and 0x0F)
        val day = ((b3 shr 4) * 10) + (b3 and 0x0F)

        return month in 1..12 && day in 1..31
    }

    private fun bcdToDateString(bytes: ByteArray): String? {
        if (bytes.size < 3) return null

        val yearByte = bytes[0].toInt() and 0xFF
        val monthByte = bytes[1].toInt() and 0xFF
        val dayByte = bytes[2].toInt() and 0xFF

        val year = ((yearByte shr 4) * 10) + (yearByte and 0x0F)
        val month = ((monthByte shr 4) * 10) + (monthByte and 0x0F)
        val day = ((dayByte shr 4) * 10) + (dayByte and 0x0F)

        if (month !in 1..12 || day !in 1..31) return null

        // Determine century
        val fullYear = if (year < 50) 2000 + year else 1900 + year

        return String.format("%04d-%02d-%02d", fullYear, month, day)
    }

    private fun readASN1Length(data: ByteArray, offset: Int): Int {
        if (offset >= data.size) return 0
        val b = data[offset].toInt() and 0xFF
        return when {
            b and 0x80 == 0 -> b
            b == 0x81 -> data[offset + 1].toInt() and 0xFF
            b == 0x82 -> ((data[offset + 1].toInt() and 0xFF) shl 8) or (data[offset + 2].toInt() and 0xFF)
            else -> b and 0x7F
        }
    }

    private fun asn1LengthByteCount(data: ByteArray, offset: Int): Int {
        if (offset >= data.size) return 0
        val b = data[offset].toInt() and 0xFF
        return when {
            b and 0x80 == 0 -> 1
            b == 0x81 -> 2
            b == 0x82 -> 3
            else -> 1 + (b and 0x7F)
        }
    }
}