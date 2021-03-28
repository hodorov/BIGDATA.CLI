package ru.hodorov.bigdatacli.aop

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.jline.utils.AttributedStyle
import org.springframework.stereotype.Component
import ru.hodorov.bigdatacli.annotation.StatusRow
import ru.hodorov.bigdatacli.service.TerminalService
import java.util.concurrent.TimeUnit


@Aspect
@Component
class ShellMethodTimeAspect(
    val terminal: TerminalService
) {
    private var startTime: Long? = null
    private var endTime: Long? = null

    @Around("@annotation(org.springframework.shell.standard.ShellMethod)")
    fun methodTimeLogger(proceedingJoinPoint: ProceedingJoinPoint): Any? {
        startTime = System.currentTimeMillis()
        var result: Any? = null
        try {
            result = proceedingJoinPoint.proceed()
        } catch (e: Throwable) {
            endTime = System.currentTimeMillis()
            terminal.println("Failed in ${formatMs(endTime!! - startTime!!)}", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT))
            throw e
        }
        endTime = System.currentTimeMillis()
        terminal.println("Processed in ${formatMs(endTime!! - startTime!!)}", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT))
        return result
    }

    @StatusRow
    fun statusRow() = "Task execution time ${if (startTime != null) formatMs((endTime ?: System.currentTimeMillis()) - startTime!!) else formatMs(0)}"

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
}
