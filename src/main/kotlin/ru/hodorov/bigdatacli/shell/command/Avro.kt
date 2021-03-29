package ru.hodorov.bigdatacli.shell.command

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
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
import java.io.BufferedInputStream

@ShellComponent
class Avro(
    val fsService: FsService,
    val fsContext: FsContext,
    @Lazy val terminal: TerminalService
) {
    private val om = jacksonObjectMapper()

    @ShellMethod("Read schema")
    fun avroSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        val file = fsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".avro") }
            .findFirst()
            .get()

        terminal.println("Use first founded file: ${file.path}")

        BufferedInputStream(fsContext.fs.open(file.path)).use { inStream ->
            val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
            val schema = reader.schema
            terminal.println("Original schema")
            terminal.println(schema.toString(true))

            val unifiedSchema = SchemaMapper.AVRO.toUnifiedModelSchema(schema)
            terminal.println("Unified schema")
            terminal.println(unifiedSchema)
        }
    }

    @ShellMethod("Read values")
    fun avroRead(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        fsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(".avro") }
            .forEach { file ->
                terminal.println("Read file ${file.path}")
                val model = SchemaMapper.AVRO.toModel(file.path, fsContext.fs)
                SchemaMapper.JSON.fromModel(model).forEachIndexed { i, row ->
                    terminal.println("File: ${file.path}, record ${i + 1}")
                    terminal.println(om.writeValueAsString(row))
                }
            }
    }
}
