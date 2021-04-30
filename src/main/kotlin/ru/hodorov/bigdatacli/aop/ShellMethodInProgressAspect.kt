package ru.hodorov.bigdatacli.aop

import org.aspectj.lang.annotation.After
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.annotation.Before
import org.aspectj.lang.annotation.Pointcut
import org.springframework.stereotype.Component

@Aspect
@Component
class ShellMethodInProgressAspect {

    var shellMethodInProgress = false

    @Pointcut("@annotation(org.springframework.shell.standard.ShellMethod)")
    fun shellMethodPointcut() {}

    @Before("shellMethodPointcut()")
    fun beforeShellMethod() {
        shellMethodInProgress = true
    }

    @After("shellMethodPointcut()")
    fun afterShellMethod() {
        shellMethodInProgress = false
    }
}
