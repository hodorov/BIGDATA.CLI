package ru.hodorov.bigdatacli.model.mapper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import ru.hodorov.bigdatacli.model.*
import java.util.*

private val om = jacksonObjectMapper()

class JsonSchemaMapper : SchemaMapper<JsonNode, Nothing, Nothing, Nothing>(
    name = "json",
    mappers = listOf(
        UnifiedFieldJavaType.INT to Mapper(null, { IntNode(it as Int) }),
        UnifiedFieldJavaType.LONG to Mapper(null, { LongNode(it as Long) }),
        UnifiedFieldJavaType.STRING to Mapper(null, { TextNode(it as String) }),
        UnifiedFieldJavaType.DATE to Mapper(null, { TextNode(om.dateFormat.format(it as Date)) }),
        UnifiedFieldJavaType.MAP to Mapper(null, { om.valueToTree(it) }),
    ),
    typeMapping = listOf(),
    subTypeMapping = listOf(),
    typePairsToUnifiedJavaType = listOf(),
    rawNull = om.nullNode()
) {

    override fun fromModel(model: UnifiedModel): List<JsonNode> {
        return model.values.map { row ->
            val objectNode = om.createObjectNode()

            row.forEach { field ->
                val value = convertUnifiedToRawValue(field.getOrDefault(), field.fieldSchema.javaType) as JsonNode
                objectNode.set<JsonNode>(field.fieldSchema.name, value)
            }

            return@map objectNode
        }
    }

    override fun toUnifiedModelSchema(schema: Nothing): UnifiedModelSchema {
        TODO("Not yet implemented")
    }

    override fun toModel(path: Path, fs: FileSystem): UnifiedModel {
        TODO("Not yet implemented")
    }

    override fun toSchema(schema: UnifiedModelSchema): Nothing {
        TODO("Not yet implemented")
    }
}
