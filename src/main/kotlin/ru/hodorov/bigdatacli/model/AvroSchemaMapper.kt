package ru.hodorov.bigdatacli.model

import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import java.lang.IllegalArgumentException

class AvroSchemaMapper {
    companion object {
        fun toUnifiedModel(schema: Schema): UnifiedModel {
            val fields = schema.fields.map {
                UnifiedField(
                    it.name(),
                    it.pos(),
                    toUnifiedType(it.schema().type),
                    toUnifiedSubType(it.schema().logicalType),
                    null,
                    it.defaultVal(),
                    true
                )
            }
            return UnifiedModel(schema.fullName, fields)
        }

        fun toUnifiedType(type: Schema.Type): UnifiedFieldType {
            return when(type) {
                Schema.Type.INT -> UnifiedFieldType.INT
                Schema.Type.LONG -> UnifiedFieldType.LONG
                Schema.Type.STRING -> UnifiedFieldType.STRING
                else -> throw IllegalArgumentException("Unknown type $type")

            }
        }

        fun toUnifiedSubType(subType: LogicalType?): UnifiedFieldSubType {
            return when(subType) {
                LogicalTypes.timestampMillis() -> UnifiedFieldSubType.TIMESTAMP_MILLIS
                null -> UnifiedFieldSubType.NONE
                else -> throw IllegalArgumentException("Unknown subType $subType")
            }
        }
    }
}
