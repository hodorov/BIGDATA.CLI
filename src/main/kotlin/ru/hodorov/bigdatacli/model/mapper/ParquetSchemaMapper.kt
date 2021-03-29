package ru.hodorov.bigdatacli.model.mapper

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import ru.hodorov.bigdatacli.model.*

class ParquetSchemaMapper : SchemaMapper<Group, MessageType, PrimitiveType, LogicalTypeAnnotation>() {
    override fun toUnifiedModelSchema(schema: MessageType): UnifiedModelSchema {
        val fields = schema.fields.map {
            UnifiedFieldSchema(
                it.name,
                schema.getFieldIndex(it.name),
                toUnifiedType(it.asPrimitiveType()),
                toUnifiedSubType(it.logicalTypeAnnotation),
                null,
                it.repetition != Type.Repetition.OPTIONAL
            )
        }
        return UnifiedModelSchema(schema.name, fields)
    }

    override fun toUnifiedType(type: PrimitiveType): UnifiedFieldType {
        return when (type.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.INT32 -> UnifiedFieldType.INT
            PrimitiveType.PrimitiveTypeName.INT64 -> UnifiedFieldType.LONG
            PrimitiveType.PrimitiveTypeName.BINARY -> UnifiedFieldType.BINARY
            else -> throw IllegalArgumentException("Unknown type $type")

        }
    }

    override fun toUnifiedSubType(subType: LogicalTypeAnnotation?): UnifiedFieldSubType {
        return when (subType) {
            is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> UnifiedFieldSubType.STRING
            null -> UnifiedFieldSubType.NONE
            else -> throw IllegalArgumentException("Unknown subType $subType")
        }
    }

    override fun toModelSchema(schema: UnifiedModelSchema): MessageType {
        TODO("Not yet implemented")
    }

    override fun toType(type: UnifiedFieldType): LogicalTypeAnnotation? {
        TODO("Not yet implemented")
    }

    override fun toSubType(subType: UnifiedFieldSubType): LogicalTypeAnnotation? {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        TODO("Not yet implemented")
    }

    override fun fromModel(model: UnifiedModel): List<Group> {
        TODO("Not yet implemented")
    }
}
