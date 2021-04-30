package ru.hodorov.bigdatacli.shell.command

import org.apache.hadoop.fs.Path
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.extension.append
import ru.hodorov.bigdatacli.extension.toHadoopPath
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FormatUtils
import ru.hodorov.bigdatacli.fs.FsContext

@ShellComponent
class Fs(
    val fsContext: FsContext,
    val fsService: FsService,
    @Lazy val terminal: TerminalService
) {
    @ShellMethod("Change directory")
    fun cd(path: String) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        if (!fsContext.fs.exists(newPath)) {
            terminal.println("Path $newPath not found")
            return
        }

        if (!fsContext.fs.isDirectory(newPath)) {
            terminal.println("Path $newPath not a folder")
            return
        }

        fsContext.currentUri = fsContext.fs.getFileStatus(newPath).path.toUri()
    }

    @ShellMethod("Calc size of file/folder")
    fun size(
        @ShellOption(defaultValue = ".") path: String
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        if (!fsContext.fs.exists(newPath)) {
            terminal.println("Path $newPath not found")
            return
        }

        val size = fsService.getFileStatusesRecursive(newPath).map { it.len }.sum()
        terminal.println("Total size ${FormatUtils.formatBytes(size)}")
    }

    @ShellMethod("Show current folder content", key = ["ls", "dir"])
    fun ls() {
        fsContext.fs.listStatus(Path(fsContext.currentUri)).forEach {
            val sb = StringBuilder()
            sb.append(
                when {
                    it.isDirectory -> " DIR"
                    it.isFile -> "FILE"
                    it.isSymlink -> "!SYMLINK!"
                    else -> "UNKNOWN"
                }
            )
            sb.append(" ")
            sb.append(fsContext.currentUri.relativize(it.path.toUri()))
            terminal.println(sb.toString())
        }
    }

    @ShellMethod("Switch FS")
    fun switch(fs: String) = fsContext.switch(fs)
}
