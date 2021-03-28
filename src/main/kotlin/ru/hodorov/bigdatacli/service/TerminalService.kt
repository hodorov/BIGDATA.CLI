package ru.hodorov.bigdatacli.service

import org.jline.terminal.Terminal
import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.jline.utils.Status
import org.springframework.stereotype.Service
import java.io.PrintWriter
import java.util.*
import kotlin.collections.HashMap
import kotlin.math.max

@Service
class TerminalService(val terminal: Terminal) : PrintWriter(terminal.writer()) {

    fun println(x: String, attributedStyle: AttributedStyle) = println(AttributedString(x, attributedStyle).toAnsi(terminal))

    fun printStringInFrame(string: String, attributedStyle: AttributedStyle = AttributedStyle.DEFAULT, frameSymbol: Char = '#') {
        val lines = string.split('\n')
        val maxLineLength = lines.stream().mapToInt { it.length }.max().asInt
        val sb = StringBuilder()
        sb.append(frameSymbol.toString().repeat(maxLineLength + 4))
        sb.append("\n")
        lines.forEach {
            sb.append("# $it${" ".repeat(maxLineLength - it.length)} #\n")
        }
        sb.append(frameSymbol.toString().repeat(maxLineLength + 4))
        this.println(AttributedString(sb.toString(), attributedStyle).toAnsi(terminal))
    }

    fun printTable(rows: List<List<String>?>, colAttributedStyle: Array<AttributedStyle?>?, colDelimiter: Char = '|') {
        val maxColWidth = HashMap<Int, Int>()
        val notNullRows = rows.filterNotNull()

        notNullRows.forEach { row ->
            row.withIndex().forEach { item ->
                val curMaxColWidth = maxColWidth[item.index] ?: 0
                val newMaxColWidth = max(item.value.length, curMaxColWidth)
                if (newMaxColWidth > curMaxColWidth) {
                    maxColWidth[item.index] = newMaxColWidth
                }
            }
        }

        notNullRows.forEach { row ->
            val rowString = row
                            .mapIndexed { index, value -> AttributedString(value + " ".repeat(maxColWidth[index]!! - value.length), colAttributedStyle?.get(index) ?: AttributedStyle.DEFAULT).toAnsi(terminal) }
                            .joinToString(colDelimiter.toString())
            this.println(rowString)
        }
    }
}
