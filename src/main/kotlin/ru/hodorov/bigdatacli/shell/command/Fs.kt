package ru.hodorov.bigdatacli.shell.command

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext

@ShellComponent
class Fs(
    val fsContext: FsContext,
    val fs: FileSystem,
    @Lazy val terminal: TerminalService
) {
    @ShellMethod("Change directory")
    fun cd(path: String) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        if (!fs.exists(newPath)) {
            terminal.println("Path $newPath not found")
            return
        }

        if (!fs.isDirectory(newPath)) {
            terminal.println("Path $newPath not a folder")
            return
        }

        fsContext.currentUri = fs.getFileStatus(newPath).path.toUri()
    }

    @ShellMethod("Show current folder content", key = ["ls", "dir"])
    fun ls() {
        fs.listStatus(Path(fsContext.currentUri)).forEach {
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
}
