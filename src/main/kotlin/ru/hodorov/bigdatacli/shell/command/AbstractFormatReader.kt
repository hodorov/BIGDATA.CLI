package ru.hodorov.bigdatacli.shell.command

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.extension.append
import ru.hodorov.bigdatacli.extension.toHadoopPath
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.fs.FsContext

class LimitReachedException : Exception()

// R - Record
// S - Schema
// T - Type
// ST - SubType
abstract class AbstractFormatReader<R, S, T, ST>(
    val fsService: FsService,
    private val fsContext: FsContext,
    val terminal: TerminalService,
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
        limit: Long,
        parseAsJson: String?
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
                        parseAsJson
                            ?.split(",")
                            ?.forEach { jsonKey ->
                                val jsonString = row.get(jsonKey)
                                if (jsonString != null) {
                                    row.set<JsonNode>(jsonKey, om.readTree(jsonString.textValue()))
                                }
                            }
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
