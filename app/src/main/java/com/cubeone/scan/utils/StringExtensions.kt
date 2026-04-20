package com.cubeone.scan.utils

fun String.decodeHex(): ByteArray {
    require(length % 2 == 0) { "Hex string must have even length" }
    return chunked(2)
        .map { it.toInt(16).toByte() }
        .toByteArray()
}

fun ByteArray.decodeToString(encoding: String = "ISO-8859-1"): String {
    return String(this, java.nio.charset.Charset.forName(encoding))
}