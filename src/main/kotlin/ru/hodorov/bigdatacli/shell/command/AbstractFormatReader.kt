package ru.hodorov.bigdatacli.shell.command

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.extends.append
import ru.hodorov.bigdatacli.extends.toHadoopPath
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.utils.FsContext
import java.lang.Exception

class LimitReachedException : Exception()

// R - Record
// S - Schema
// T - Type
// ST - SubType
abstract class AbstractFormatReader<R, S, T, ST>(
    private val fsService: FsService,
    private val fsContext: FsContext,
    private val terminal: TerminalService,
    private val mapper: SchemaMapper<R, S, T, ST>,
    private val fileSuffix: String
) {
    private val om = jacksonObjectMapper()
    private val prettifyOm = jacksonObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)

    abstract fun readSchemaFromPath(path: Path): S

    fun readSchema(path: String) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()
        val file = fsService.getFileStatusesRecursiveStream(newPath)
            .filter { it.path.toString().endsWith(fileSuffix) }
            .findFirst()
            .get()

        terminal.println("Use first founded file: ${file.path}")

        val schema = readSchemaFromPath(file.path)

        terminal.println("Original schema")
        terminal.println(schema)
        terminal.println("")

        val unifiedSchema = mapper.toUnifiedModelSchema(schema)
        terminal.println("Unified schema")
        terminal.println(unifiedSchema)
        terminal.println("")
    }

    fun readRecords(
        path: String,
        prettify: Boolean,
        limit: Long
    ) {
        val newPath = fsContext.currentUri.append(path).toHadoopPath()

        try {
            var totalRows = 0L
            fsService.getFileStatusesRecursiveStream(newPath)
                .filter { it.path.toString().endsWith(fileSuffix) }
                .forEach { file ->
                    terminal.println("Read file ${file.path}")
                    val model = mapper.toModel(file.path, fsContext.fs)
                    SchemaMapper.JSON.fromModel(model).forEachIndexed { i, row ->
                        terminal.println("File: ${file.path}, record ${i + 1}")
                        terminal.println((if (prettify) prettifyOm else om).writeValueAsString(row))
                        totalRows++
                        if (limit in 1..totalRows) {
                            throw LimitReachedException()
                        }
                    }
                }
        } catch (e: LimitReachedException) {
            terminal.println("Limit reached")
        }
    }
}
