package ru.hodorov.bigdatacli.model.mapper

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*

// R - Record
// S - Schema
// T - Type
// ST - SubType
abstract class SchemaMapper<R, S, T, ST> {

    abstract fun toUnifiedModelSchema(schema: S): UnifiedModelSchema
    abstract fun toUnifiedType(type: T): UnifiedFieldType
    abstract fun toUnifiedSubType(subType: ST?): UnifiedFieldSubType

    abstract fun toModelSchema(schema: UnifiedModelSchema): S
    abstract fun toType(type: UnifiedFieldType): ST?
    abstract fun toSubType(subType: UnifiedFieldSubType): ST?

    abstract fun toUnifiedFieldJavaType(value: Any, unifiedFieldJavaType: UnifiedFieldJavaType): Any

    abstract fun toModel(path: Path, fs: FileSystem): UnifiedModel
    abstract fun fromModel(model: UnifiedModel): List<R>

    companion object {
        val AVRO = AvroSchemaMapper()
        val PARQUET = ParquetSchemaMapper()
        val JSON = JsonSchemaMapper()
    }
}
