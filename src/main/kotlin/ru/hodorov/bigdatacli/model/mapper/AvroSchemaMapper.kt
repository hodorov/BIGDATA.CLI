package ru.hodorov.bigdatacli.model.mapper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.TextNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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

private val om = jacksonObjectMapper()

class AvroSchemaMapper : SchemaMapper<GenericData.Record, Schema, Schema.Type, LogicalType>(
    name = "avro",
    mappers = listOf(
        UnifiedFieldJavaType.STRING to Mapper({ TextNode(it.toString()) /*Utf8 -> String*/ }, null),
        UnifiedFieldJavaType.DATE to Mapper({ LongNode(it as Long) }, null),
        UnifiedFieldJavaType.LONG to Mapper({ LongNode(it as Long) }, null),
        UnifiedFieldJavaType.MAP to Mapper({ om.valueToTree(it) }, null),
    ),
    typeMapping = listOf(
        Schema.Type.INT to UnifiedFieldType.INT,
        Schema.Type.LONG to UnifiedFieldType.LONG,
        Schema.Type.STRING to UnifiedFieldType.STRING,
        Schema.Type.MAP to UnifiedFieldType.MAP,
    ),
    subTypeMapping = listOf(
        LogicalTypes.timestampMillis() to UnifiedFieldSubType.TIMESTAMP_MILLIS,
    ),
    typePairsToUnifiedJavaType = listOf(
        (UnifiedFieldType.STRING to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.STRING,
        (UnifiedFieldType.INT to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.INT,
        (UnifiedFieldType.LONG to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.LONG,
        (UnifiedFieldType.LONG to UnifiedFieldSubType.TIMESTAMP_MILLIS) to UnifiedFieldJavaType.DATE,
        (UnifiedFieldType.MAP to UnifiedFieldSubType.NONE) to UnifiedFieldJavaType.MAP,
    )
) {

    override fun toUnifiedModelSchema(schema: Schema): UnifiedModelSchema {
        val fields = schema.fields.map {
            val type = toUnifiedType(it.schema().type)
            val subType = toUnifiedSubType(it.schema().logicalType)
            UnifiedFieldSchema(
                it.name(),
                it.pos(),
                type,
                subType,
                toUnifiedJavaType(type to subType),
                defaultToJsonNode(it.defaultVal()),
                true
            )
        }
        return UnifiedModelSchema(schema.fullName, fields)
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        BufferedInputStream(fs.open(path)).use { inStream ->
            val reader: DataFileStream<GenericData.Record> = DataFileStream(inStream, GenericDatumReader())
            val unifiedSchema = toUnifiedModelSchema(reader.schema)
            val values = reader.map { row ->
                unifiedSchema.fields.map field@{ fieldSchema ->
                    val value = row.get(fieldSchema.position) ?: fieldSchema.default
                    return@field UnifiedField(fieldSchema, value?.let { convertRawValueToUnified(it, fieldSchema.javaType) }, value)
                }
            }
            return UnifiedModel(path, unifiedSchema, values)
        }
    }

    override fun toSchema(schema: UnifiedModelSchema): Schema {
        TODO("Not yet implemented")
    }

    override fun fromModel(model: UnifiedModel): List<GenericData.Record> {
        TODO("Not yet implemented")
    }

    private fun defaultToJsonNode(default: Any?): JsonNode? {
        if (default != null) {
            TODO("Not yet implemented")
        } else {
            return null
        }
    }
}
