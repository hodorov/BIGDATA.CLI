package ru.hodorov.bigdatacli.utils

import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Service

@Service
class ApplicationContextUtils(
    val applicationContext: ApplicationContext
) {
    fun <T: Any> recreateBean(instance: T): T {
        val clazz = instance::class.java
        return applicationContext.autowireCapableBeanFactory.createBean(clazz)
    }
}
