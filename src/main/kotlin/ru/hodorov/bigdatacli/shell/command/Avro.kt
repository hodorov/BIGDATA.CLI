package ru.hodorov.bigdatacli.shell.command

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs.FileSystem
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.model.AvroSchemaMapper
import ru.hodorov.bigdatacli.service.HdfsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext
import java.io.BufferedInputStream

@ShellComponent
class Avro(
    val hdfsService: HdfsService,
    val fsContext: FsContext,
    @Lazy val terminal: TerminalService
) {
    @ShellMethod("Read schema")
    fun avroSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        val file = hdfsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".avro") }
            .findFirst()
            .get()

        terminal.println("Use first founded file: ${file.path}")

        BufferedInputStream(fsContext.fs.open(file.path)).use { inStream ->
            val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
            val schema = reader.schema
            terminal.println("Original schema")
            terminal.println(schema.toString(true))

            val unifiedSchema = AvroSchemaMapper.toUnifiedModel(schema)
            terminal.println("Unified schema")
            terminal.println(unifiedSchema)
        }
    }
}
