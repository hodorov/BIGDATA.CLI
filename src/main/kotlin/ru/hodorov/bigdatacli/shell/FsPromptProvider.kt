package ru.hodorov.bigdatacli.shell

import org.jline.utils.AttributedString
import org.jline.utils.AttributedStyle
import org.springframework.shell.jline.PromptProvider
import org.springframework.shell.standard.ShellComponent
import ru.hodorov.bigdatacli.utils.FsContext

@ShellComponent
class FsPromptProvider(
    val fsContext: FsContext
) : PromptProvider {
    override fun getPrompt() = AttributedString("${fsContext.currentUri}:>", AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
}
