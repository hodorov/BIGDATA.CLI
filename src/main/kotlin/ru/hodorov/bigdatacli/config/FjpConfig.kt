package ru.hodorov.bigdatacli.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.hodorov.bigdatacli.annotation.StatusRow
import java.util.concurrent.ForkJoinPool

@Configuration
class FjpConfig(
    @Value("\${async.fjp.parallelism}")
    @Suppress("SpringJavaInjectionPointsAutowiringInspection")
    val parallelism: Int
) {
    private val fjp = ForkJoinPool(parallelism)

    @Bean
    fun fjp() = fjp

    @StatusRow
    fun statusRow(): String = "FJP PS ${fjp.poolSize} A ${fjp.activeThreadCount} R ${fjp.runningThreadCount} P ${fjp.parallelism}"
}
