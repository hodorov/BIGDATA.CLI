package ru.hodorov.bigdatacli.utils

import java.util.concurrent.TimeUnit
import java.text.StringCharacterIterator

import java.text.CharacterIterator
import kotlin.math.abs


class FormatUtils {
    companion object {
        fun formatMs(millis: Long): String {
            if (millis == 0L) {
                return "0ms"
            }
            val sb = StringBuilder()
            TimeUnit.MILLISECONDS.toDays(millis).takeIf { it > 0 }?.apply { sb.append("${this}d") }
            (TimeUnit.MILLISECONDS.toHours(millis) % 24).takeIf { it > 0 }?.apply { sb.append("${this}h") }
            (TimeUnit.MILLISECONDS.toMinutes(millis) % 60).takeIf { it > 0 }?.apply { sb.append("${this}m") }
            (TimeUnit.MILLISECONDS.toSeconds(millis) % 60).takeIf { it > 0 }?.apply { sb.append("${this}s") }
            (millis % 1000).takeIf { it > 0 }?.apply { sb.append("${this}ms") }
            return sb.toString()
        }

        fun formatBytes(bytes: Long): String? {
            val absB = if (bytes == Long.MIN_VALUE) Long.MAX_VALUE else abs(bytes)
            if (absB < 1024) {
                return "$bytes B"
            }
            var value = absB
            val ci: CharacterIterator = StringCharacterIterator("KMGTPE")
            var i = 40
            while (i >= 0 && absB > 0xfffccccccccccccL shr i) {
                value = value shr 10
                ci.next()
                i -= 10
            }
            value *= java.lang.Long.signum(bytes).toLong()
            return String.format("%.1f %ciB", value / 1024.0, ci.current())
        }
    }
}
