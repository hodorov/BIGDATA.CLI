package ru.hodorov.bigdatacli.shell.command

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.springframework.context.annotation.Lazy
import org.springframework.shell.standard.ShellComponent
import org.springframework.shell.standard.ShellMethod
import org.springframework.shell.standard.ShellOption
import ru.hodorov.bigdatacli.model.mapper.SchemaMapper
import ru.hodorov.bigdatacli.service.FsService
import ru.hodorov.bigdatacli.service.TerminalService
import ru.hodorov.bigdatacli.fs.FsContext

@ShellComponent
class Parquet(
    fsService: FsService,
    val fsContext: FsContext,
    @Lazy terminal: TerminalService
) : AbstractFormatReader<Group, MessageType, PrimitiveType.PrimitiveTypeName, Class<out LogicalTypeAnnotation>>(
    fsService,
    fsContext,
    terminal,
    SchemaMapper.PARQUET,
    ".parquet"
) {

    override fun readSchemaFromPath(path: Path): MessageType {
        return ParquetFileReader.open(HadoopInputFile.fromPath(path, fsContext.fs.conf)).use { reader ->
            return@use reader.footer.fileMetaData.schema
        }
    }

    @ShellMethod("Read schema")
    fun parquetSchema(
        @ShellOption(defaultValue = ".") path: String,
    ) {
        readSchema(path)
    }
}
