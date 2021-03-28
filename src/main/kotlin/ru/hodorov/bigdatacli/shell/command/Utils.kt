package ru.hodorov.bigdatacli.shell.command

import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod

@ShellComponent
class Utils {
    @ShellMethod("Sleep")
    fun sleep(ms: Long) {
        Thread.sleep(ms)
    }

    @ShellMethod("Throw")
    fun throwMe() {
        throw IllegalStateException()
    }
}
