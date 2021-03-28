package ru.hodorov.bigdatacli.model

import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type

class ParquetSchemaMapper {
    companion object {
        fun toUnifiedModel(schema: MessageType): UnifiedModel {
            val fields = schema.fields.map {
                UnifiedField(
                    it.name,
                    schema.getFieldIndex(it.name),
                    toUnifiedType(it.asPrimitiveType()),
                    toUnifiedSubType(it.logicalTypeAnnotation),
                    null,
                    null,
                    it.repetition != Type.Repetition.OPTIONAL
                )
            }
            return UnifiedModel(schema.name, fields)
        }

        fun toUnifiedType(type: PrimitiveType): UnifiedFieldType {
            return when (type.primitiveTypeName) {
                PrimitiveType.PrimitiveTypeName.INT32 -> UnifiedFieldType.INT
                PrimitiveType.PrimitiveTypeName.INT64 -> UnifiedFieldType.LONG
                PrimitiveType.PrimitiveTypeName.BINARY -> UnifiedFieldType.BINARY
                else -> throw IllegalArgumentException("Unknown type $type")

            }
        }

        fun toUnifiedSubType(subType: LogicalTypeAnnotation?): UnifiedFieldSubType {
            return when (subType) {
                is LogicalTypeAnnotation.StringLogicalTypeAnnotation -> UnifiedFieldSubType.STRING
                null -> UnifiedFieldSubType.NONE
                else -> throw IllegalArgumentException("Unknown subType $subType")
            }
        }
    }
}
