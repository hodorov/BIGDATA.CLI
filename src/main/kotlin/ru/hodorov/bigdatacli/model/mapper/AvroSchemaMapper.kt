package ru.hodorov.bigdatacli.model.mapper

import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*
import java.io.BufferedInputStream

class AvroSchemaMapper : SchemaMapper<GenericData.Record, Schema, Schema.Type, LogicalType>() {

    override fun toUnifiedModelSchema(schema: Schema): UnifiedModelSchema {
        val fields = schema.fields.map {
            UnifiedFieldSchema(
                it.name(),
                it.pos(),
                toUnifiedType(it.schema().type),
                toUnifiedSubType(it.schema().logicalType),
                it.defaultVal(),
                true
            )
        }
        return UnifiedModelSchema(schema.fullName, fields)
    }

    override fun toUnifiedType(type: Schema.Type): UnifiedFieldType {
        return when (type) {
            Schema.Type.INT -> UnifiedFieldType.INT
            Schema.Type.LONG -> UnifiedFieldType.LONG
            Schema.Type.STRING -> UnifiedFieldType.STRING
            else -> throw IllegalArgumentException("Unknown type $type")

        }
    }

    override fun toUnifiedSubType(subType: LogicalType?): UnifiedFieldSubType {
        return when (subType) {
            LogicalTypes.timestampMillis() -> UnifiedFieldSubType.TIMESTAMP_MILLIS
            null -> UnifiedFieldSubType.NONE
            else -> throw IllegalArgumentException("Unknown subType $subType")
        }
    }

    override fun toModelSchema(schema: UnifiedModelSchema): Schema {
        TODO("Not yet implemented")
    }

    override fun toType(type: UnifiedFieldType): LogicalType? {
        TODO("Not yet implemented")
    }

    override fun toSubType(subType: UnifiedFieldSubType): LogicalType? {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        BufferedInputStream(fs.open(path)).use { inStream ->
            val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
            val unifiedSchema = toUnifiedModelSchema(reader.schema)
            val values = reader.map { row ->
                unifiedSchema.fields.map { field ->
                    UnifiedField(field, row.get(field.position))
                }
            }
            return UnifiedModel(path, unifiedSchema, values)
        }
    }

    override fun fromModel(model: UnifiedModel): List<GenericData.Record> {
        TODO("Not yet implemented")
    }
}
