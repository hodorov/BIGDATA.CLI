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
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext
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
    ) {
        readRecords(path, prettify, limit)
    }
}
