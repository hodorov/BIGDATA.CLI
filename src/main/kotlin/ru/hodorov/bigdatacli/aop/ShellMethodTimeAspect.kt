package ru.hodorov.bigdatacli.aop

import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.Around
import org.aspectj.lang.annotation.Aspect
import org.jline.utils.AttributedStyle
import org.springframework.stereotype.Component
import ru.hodorov.bigdatacli.annotation.StatusRow
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FormatUtils
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
        val result: Any?
        try {
            result = proceedingJoinPoint.proceed()
        } catch (e: Throwable) {
            endTime = System.currentTimeMillis()
            terminal.println("Failed in ${FormatUtils.formatMs(endTime!! - startTime!!)}", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT))
            throw e
        }
        endTime = System.currentTimeMillis()
        terminal.println("Processed in ${FormatUtils.formatMs(endTime!! - startTime!!)}", AttributedStyle.DEFAULT.foreground(AttributedStyle.BRIGHT))
        return result
    }

    @StatusRow
    fun statusRow() = "Task execution time ${if (startTime != null) FormatUtils.formatMs((endTime ?: System.currentTimeMillis()) - startTime!!) else FormatUtils.formatMs(0)}"
}
