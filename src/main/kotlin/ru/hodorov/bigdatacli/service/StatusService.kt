package ru.hodorov.bigdatacli.service

import mu.KotlinLogging
import org.jline.terminal.Terminal
import org.jline.terminal.impl.AbstractTerminal
import org.jline.utils.AttributedString
import org.jline.utils.Status
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.Ordered
import org.springframework.core.annotation.AnnotationUtils
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Service
import ru.hodorov.bigdatacli.annotation.StatusRow
import ru.hodorov.bigdatacli.aop.ShellMethodInProgressAspect
import java.lang.reflect.Method
import javax.annotation.PostConstruct
import kotlin.concurrent.thread


private val log = KotlinLogging.logger { }


@Service
class StatusService(
    @Value("\${terminal.status.supported.refreshRate}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val supportedRefreshRate: Long,
    @Value("\${terminal.status.unsupported.refreshRate}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val unsupportedRefreshRate: Long,
    terminal: Terminal,
    val shellMethodInProgressAspect: ShellMethodInProgressAspect,
    val applicationContext: GenericApplicationContext
) : Status(terminal as AbstractTerminal) {

    private var statusRows: List<Pair<Any, Method>>? = null

    @PostConstruct
    fun postConstruct() {
        if (!supported) {
            log.warn { "Status row not supported in you terminal" }
        }
        val refreshRate = if (supported) supportedRefreshRate else unsupportedRefreshRate

        log.debug { "Start status row updater with rate = ${refreshRate}ms" }
        thread(name = "Status row", isDaemon = true) {
            while (true) {
                try {
                    if (statusRows != null) {
                        val rows = statusRows!!.map {
                            return@map when (val ret = it.second.invoke(it.first)) {
                                is String -> AttributedString(ret)
                                is AttributedString -> ret
                                else -> throw IllegalArgumentException("Unknown ret type ${ret::class} from ${it.second}")
                            }
                        }
                        update(rows)
                    }
                } catch (e: Throwable) {
                    log.error(e) { "Status row error" }
                }
                Thread.sleep(refreshRate)
            }
        }
    }

    @EventListener(ContextRefreshedEvent::class)
    fun onContextRefreshed() {
        val statusRowsTmp = ArrayList<Triple<Any, Method, Int>>()
        for (beanName in applicationContext.beanDefinitionNames) {
            val beanDefinition = applicationContext.getBeanDefinition(beanName!!)
            if (beanDefinition.beanClassName != null) {
                val beanClass = Class.forName(beanDefinition.beanClassName)
                for (method in beanClass.methods) {
                    if (AnnotationUtils.findAnnotation(method, StatusRow::class.java) != null) {
                        val priority = AnnotationUtils.findAnnotation(method, Order::class.java)?.value ?: Ordered.LOWEST_PRECEDENCE
                        log.debug { "Find new status row method $method with priority $priority" }
                        statusRowsTmp.add(Triple(applicationContext.getBean(beanName), method, priority))
                    }
                }
            }
        }
        statusRows = statusRowsTmp.sortedBy { it.third }.map { it.first to it.second }
    }

    override fun update(lines: List<AttributedString>) {
        if (supported) {
            super.update(lines)
        } else if (shellMethodInProgressAspect.shellMethodInProgress) {
            // Can't use terminal.writer().println(...) because terminal may be busy (Thread.sleep(), as ex.)
            lines.forEach { println("(STATUS) ${it.toAnsi(terminal)}") }
        }
    }
}
