package ru.hodorov.bigdatacli.shell.command

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs.FileSystem
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.service.HdfsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext
import java.io.BufferedInputStream

@ShellComponent
class Parquet(
    val hdfsService: HdfsService,
    val fsContext: FsContext,
    @Lazy val terminal: TerminalService
) {
    @ShellMethod("Read schema")
    fun parquetSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        val file = hdfsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".parquet") }
            .findFirst()
            .get()

        terminal.println("Use first founded file: ${file.path}")

        ParquetFileReader.open(HadoopInputFile.fromPath(file.path, fsContext.fs.conf)).use { reader ->
            val schema = reader.footer.fileMetaData.schema
            terminal.println(schema)
        }
    }
}
