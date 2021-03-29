package ru.hodorov.bigdatacli.model.mapper

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import ru.hodorov.bigdatacli.model.*

class ParquetSchemaMapper : SchemaMapper<Group, MessageType, PrimitiveType.PrimitiveTypeName, LogicalTypeAnnotation>(
    name = "parquet",
    mappers = listOf(), // TODO
    typeMapping = listOf(
        PrimitiveType.PrimitiveTypeName.INT32 to UnifiedFieldType.INT,
        PrimitiveType.PrimitiveTypeName.INT64 to UnifiedFieldType.LONG,
        PrimitiveType.PrimitiveTypeName.BINARY to UnifiedFieldType.BINARY,
    ),
    subTypeMapping = listOf(), // Overrided
    typePairsToUnifiedJavaType = listOf(
        (UnifiedFieldType.INT to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.INT,
        (UnifiedFieldType.LONG to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.LONG,
        (UnifiedFieldType.BINARY to UnifiedFieldSubType.STRING) to UnifiedFieldJavaType.STRING,
    )
) {

    override fun toUnifiedModelSchema(schema: MessageType): UnifiedModelSchema {
        val fields = schema.fields.map {
            val type = toUnifiedType(it.asPrimitiveType().primitiveTypeName)
            val subType = toUnifiedSubType(it.logicalTypeAnnotation)

            return@map UnifiedFieldSchema(
                it.name,
                schema.getFieldIndex(it.name),
                type,
                subType,
                toUnifiedJavaType(type to subType),
                null,
                it.repetition != Type.Repetition.OPTIONAL
            )
        }
        return UnifiedModelSchema(schema.name, fields)
    }

    override fun toUnifiedSubType(subType: LogicalTypeAnnotation?): UnifiedFieldSubType {
        return when (subType) {
            is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> UnifiedFieldSubType.STRING
            null -> UnifiedFieldSubType.NONE
            else -> throw IllegalArgumentException("Unknown subType $subType")
        }
    }

    override fun toSubType(subType: UnifiedFieldSubType): LogicalTypeAnnotation? {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        TODO("Not yet implemented")
    }

    override fun toSchema(schema: UnifiedModelSchema): MessageType {
        TODO("Not yet implemented")
    }

    override fun fromModel(model: UnifiedModel): List<Group> {
        TODO("Not yet implemented")
    }
}
