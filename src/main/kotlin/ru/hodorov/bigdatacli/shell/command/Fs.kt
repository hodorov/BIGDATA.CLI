package ru.hodorov.bigdatacli.shell.command

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
    fun switch(fs: String) {
        fsContext.activeFs = FsContext.ACTIVE_FS.valueOf(fs.toUpperCase())
    }
}
