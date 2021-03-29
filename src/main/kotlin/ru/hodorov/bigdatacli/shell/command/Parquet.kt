package ru.hodorov.bigdatacli.shell.command

import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext

@ShellComponent
class Parquet(
    val fsService: FsService,
    val fsContext: FsContext,
    @Lazy val terminal: TerminalService
) {
    @ShellMethod("Read schema")
    fun parquetSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        val file = fsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".parquet") }
            .findFirst()
            .get()

        terminal.println("Use first founded file: ${file.path}")

        ParquetFileReader.open(HadoopInputFile.fromPath(file.path, fsContext.fs.conf)).use { reader ->
            val schema = reader.footer.fileMetaData.schema

            terminal.println("Original schema")
            terminal.println(schema)

            val unifiedSchema = SchemaMapper.PARQUET.toUnifiedModelSchema(schema)
            terminal.println("Unified schema")
            terminal.println(unifiedSchema)
        }
    }
}
