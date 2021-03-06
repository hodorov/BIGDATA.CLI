package ru.hodorov.bigdatacli.shell.command

import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs.Path
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.extension.append
import ru.hodorov.bigdatacli.extension.toHadoopPath
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.fs.FsContext
import java.io.BufferedInputStream

@ShellComponent
class Avro(
    fsService: FsService,
    val fsContext: FsContext,
    @Lazy terminal: TerminalService
) : AbstractFormatReader<GenericData.Record, Schema, Schema.Type, LogicalType>(
    fsService,
    fsContext,
    terminal,
    SchemaMapper.AVRO,
    ".avro",
) {

    override fun readSchemaFromPath(path: Path): Schema {
        return BufferedInputStream(fsContext.fs.open(path)).use { inStream ->
            val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
            return@use reader.schema
        }
    }

    @ShellMethod("Read schema")
    fun avroSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        readSchema(path)
    }

    @ShellMethod("Read values")
    fun avroRead(
        @ShellOption(defaultValue = ".") path: String,
        @ShellOption(defaultValue = "false") prettify: Boolean,
        @ShellOption(defaultValue = "-1") limit: Long,
        @ShellOption(defaultValue = "") parseAsJson: String,
    ) {
        readRecords(path, prettify, limit, parseAsJson.takeIf { it.isNotBlank() })
    }

    @ShellMethod("Count values")
    fun avroCount(
        @ShellOption(defaultValue = ".") path: String
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()

        var totalCount = 0

        fsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".avro") }
            .parallel()
            .forEach { file ->
                BufferedInputStream(fsContext.fs.open(file.path)).use { inStream ->
                    val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
                    terminal.println("File ${file.path}")
                    val count = reader.count()
                    totalCount += count
                    terminal.println("Count ${count}, total count ${totalCount}")
                }
            }
    }
}
